from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

from integration_hub.config import get_settings
from integration_hub.core.base import (
    Capability,
    ConnectionContext,
    Connector,
    Cursor,
    ReadPage,
    Record,
    StreamSpec,
    WebhookEvent,
    WriteMode,
    WriteOutcome,
    WriteResult,
)
from integration_hub.core.errors import ConfigurationError, ValidationFailed
from integration_hub.core.mapping import ObjectMapping, get_transform
from integration_hub.core.registry import register_connector, register_oauth_provider
from integration_hub.logging_config import get_logger
from integration_hub.providers.zoho_crm import auth as zauth
from integration_hub.providers.zoho_crm.client import MAX_WRITE_BATCH, ZohoCrmClient
from integration_hub.providers.zoho_crm.mappings import DEDUPE_FIELDS, MAPPINGS, OBJECT_TYPES

log = get_logger(__name__)

_to_datetime = get_transform("datetime")

# Zoho notification channels live at most 24h, so we renew well before expiry.
CHANNEL_TTL = timedelta(hours=23)


@register_connector
class ZohoCrmConnector(Connector):
    provider_key = "zoho_crm"
    display_name = "Zoho CRM"
    capabilities = {
        Capability.READ,
        Capability.INCREMENTAL_READ,
        Capability.WRITE,
        Capability.UPSERT,
        Capability.DELETE,
        Capability.WEBHOOK,
        Capability.SCHEMA_DISCOVERY,
    }

    def __init__(self, ctx: ConnectionContext):
        super().__init__(ctx)
        self.client = ZohoCrmClient(ctx)

    async def aclose(self) -> None:
        await self.client.aclose()

    # ------------------------------------------------------------------ discovery
    def streams(self) -> list[StreamSpec]:
        return [
            StreamSpec(
                name=name,
                object_type=OBJECT_TYPES[name],
                capabilities=self.capabilities,
                cursor_field="Modified_Time",
                dedupe_fields=DEDUPE_FIELDS.get(name, []),
            )
            for name in MAPPINGS
        ]

    def mapping(self, stream: str) -> ObjectMapping:
        try:
            return MAPPINGS[stream]
        except KeyError as exc:
            raise ConfigurationError(f"Zoho CRM stream '{stream}' is not configured") from exc

    async def check_connection(self) -> dict[str, Any]:
        org = await self.client.get_org()
        info = (org.get("org") or [{}])[0]
        return {
            "ok": True,
            "org_id": info.get("zgid"),
            "company_name": info.get("company_name"),
            "api_domain": self.client.tokens.api_domain,
            "dc": self.client.tokens.dc,
        }

    async def describe(self, stream: str) -> list[dict[str, Any]]:
        module = self.mapping(stream).remote_object
        payload = await self.client.list_fields(module)
        return [
            {
                "api_name": f.get("api_name"),
                "label": f.get("field_label"),
                "type": f.get("data_type"),
                "required": f.get("system_mandatory", False),
                "read_only": f.get("read_only", False),
                "picklist": [p.get("actual_value") for p in (f.get("pick_list_values") or [])] or None,
            }
            for f in payload.get("fields", [])
        ]

    # ------------------------------------------------------------------ read
    async def read(self, stream: str, cursor: Cursor, *, page_size: int) -> AsyncIterator[ReadPage]:
        mapping = self.mapping(stream)
        module = mapping.remote_object

        # Webhook-driven hydration: fetch exactly the notified ids.
        ids = (cursor.extra or {}).get("ids")
        if ids:
            payload = await self.client.get_records_by_ids(module, ids, fields=self._remote_fields(mapping))
            yield ReadPage(
                records=[self._to_record(mapping, r) for r in payload.get("data", [])],
                cursor=cursor,
                has_more=False,
            )
            return

        modified_since = self._if_modified_since(cursor)
        page_token = cursor.page_token
        high_water = cursor.since

        while True:
            payload = await self.client.get_records(
                module,
                per_page=page_size,
                page_token=page_token,
                fields=self._remote_fields(mapping),
                modified_since=modified_since,
            )
            rows = payload.get("data") or []
            if not rows:
                break

            records = [self._to_record(mapping, r) for r in rows]
            for rec in records:
                if rec.updated_at and (high_water is None or rec.updated_at > high_water):
                    high_water = rec.updated_at

            info = payload.get("info") or {}
            page_token = info.get("next_page_token")
            has_more = bool(info.get("more_records")) and bool(page_token)

            yield ReadPage(
                records=records,
                cursor=Cursor(since=high_water, page_token=page_token if has_more else None),
                has_more=has_more,
            )
            if not has_more:
                break

        async for page in self._read_deleted(stream, module, high_water):
            yield page

    async def _read_deleted(
        self, stream: str, module: str, high_water: datetime | None
    ) -> AsyncIterator[ReadPage]:
        """Zoho hard-deletes are only visible through the /deleted endpoint."""
        if not self.ctx.config.get("track_deletes", True):
            return
        page_token: str | None = None
        while True:
            payload = await self.client.get_deleted(module, page_token=page_token)
            rows = payload.get("data") or []
            if not rows:
                return
            records = [
                Record(
                    external_id=str(row.get("id")),
                    raw=row,
                    canonical={"external_id": str(row.get("id"))},
                    updated_at=_to_datetime(row.get("deleted_time")),
                    deleted=True,
                )
                for row in rows
            ]
            info = payload.get("info") or {}
            page_token = info.get("next_page_token")
            has_more = bool(info.get("more_records")) and bool(page_token)
            yield ReadPage(
                records=records, cursor=Cursor(since=high_water, page_token=None), has_more=has_more
            )
            if not has_more:
                return

    def _to_record(self, mapping: ObjectMapping, row: dict) -> Record:
        canonical = mapping.to_canonical(row)
        return Record(
            external_id=str(row.get(mapping.id_field)) if row.get(mapping.id_field) else None,
            raw=row,
            canonical=canonical,
            updated_at=_to_datetime(row.get(mapping.updated_field or "Modified_Time")),
        )

    @staticmethod
    def _remote_fields(mapping: ObjectMapping) -> list[str]:
        return list(dict.fromkeys(field.remote.split(".")[0] for field in mapping.fields))

    @staticmethod
    def _if_modified_since(cursor: Cursor) -> str | None:
        if not cursor.since:
            return None
        dt = cursor.since if cursor.since.tzinfo else cursor.since.replace(tzinfo=timezone.utc)
        # Zoho expects ISO-8601 with an explicit offset, e.g. 2026-01-31T10:00:00+00:00
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

    # ------------------------------------------------------------------ write
    async def write(self, stream: str, records: list[dict], *, mode: WriteMode) -> WriteResult:
        mapping = self.mapping(stream)
        module = mapping.remote_object
        spec = self.stream(stream)

        if mode is WriteMode.DELETE:
            ids = [str(r.get("external_id")) for r in records if r.get("external_id")]
            if not ids:
                raise ValidationFailed("Delete requires external_id on every record", provider=self.provider_key)
            payload = await self.client.delete(module, ids)
            return self._to_result(payload, fallback_ids=ids)

        remote_rows = []
        for rec in records[:MAX_WRITE_BATCH]:
            row = mapping.to_remote(rec)
            if rec.get("external_id"):
                row["id"] = str(rec["external_id"])
            remote_rows.append(row)

        if mode is WriteMode.INSERT:
            payload = await self.client.insert(module, remote_rows)
        elif mode is WriteMode.UPDATE:
            payload = await self.client.update(module, remote_rows)
        else:
            payload = await self.client.upsert(
                module, remote_rows, duplicate_check_fields=spec.dedupe_fields or None
            )
        return self._to_result(payload)

    @staticmethod
    def _to_result(payload: dict, fallback_ids: list[str] | None = None) -> WriteResult:
        outcomes: list[WriteOutcome] = []
        for idx, entry in enumerate(payload.get("data") or []):
            details = entry.get("details") or {}
            code = entry.get("code")
            external_id = details.get("id") or (fallback_ids[idx] if fallback_ids else None)
            if entry.get("status") == "success":
                # /upsert reports which branch it took via "action"; plain insert/update do not.
                action = entry.get("action")
                outcomes.append(
                    WriteOutcome(
                        external_id=str(external_id) if external_id else None,
                        status="created" if action in (None, "insert") else "updated",
                        code=code,
                    )
                )
            else:
                outcomes.append(
                    WriteOutcome(
                        external_id=str(external_id) if external_id else None,
                        status="failed",
                        code=code,
                        error=f"{code}: {entry.get('message')} {details or ''}".strip(),
                    )
                )
        return WriteResult(outcomes=outcomes)

    # ------------------------------------------------------------------ webhooks
    async def register_webhook(self, streams: list[str], callback_url: str) -> dict[str, Any]:
        settings = get_settings()
        channel_id = str(uuid.uuid4().int % 10**15)
        events: list[str] = []
        for stream in streams:
            module = self.mapping(stream).remote_object
            events += [f"{module}.create", f"{module}.edit", f"{module}.delete"]

        expiry = (datetime.now(timezone.utc) + CHANNEL_TTL).isoformat(timespec="seconds")
        payload = {
            "watch": [
                {
                    "channel_id": channel_id,
                    "events": events,
                    "channel_expiry": expiry,
                    "token": settings.zoho_webhook_token or self.ctx.connection_id,
                    "notify_url": callback_url,
                }
            ]
        }
        response = await self.client.watch(payload)
        return {
            "channel_id": channel_id,
            "expires_at": expiry,
            "streams": streams,
            "callback_url": callback_url,
            "response": response,
        }

    async def unregister_webhook(self, subscription: dict[str, Any]) -> None:
        await self.client.unwatch([subscription["channel_id"]])

    def parse_webhook(self, headers: dict[str, str], body: dict) -> list[WebhookEvent]:
        """Zoho posts {"module":..,"operation":..,"ids":[..],"token":..,"channel_id":..}."""
        expected = get_settings().zoho_webhook_token or self.ctx.connection_id
        if expected and body.get("token") != expected:
            raise ValidationFailed("Zoho notification token mismatch", provider=self.provider_key)

        module = body.get("module") or ""
        stream = next((s for s, m in MAPPINGS.items() if m.remote_object == module), module.lower())
        operation = (body.get("operation") or "edit").lower()
        occurred = _to_datetime(body.get("server_time") or body.get("operation_time"))

        events = []
        for external_id in body.get("ids") or []:
            seed = f"{body.get('channel_id')}:{module}:{operation}:{external_id}:{body.get('server_time', '')}"
            events.append(
                WebhookEvent(
                    provider=self.provider_key,
                    connection_id=self.ctx.connection_id,
                    stream=stream,
                    external_id=str(external_id),
                    operation="delete" if operation == "delete" else ("create" if operation == "insert" else "update"),
                    occurred_at=occurred,
                    raw=body,
                    dedupe_key=hashlib.sha256(seed.encode()).hexdigest(),
                )
            )
        return events


register_oauth_provider("zoho_crm", zauth)

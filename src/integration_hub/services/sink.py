from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.core.base import Record, StreamSpec
from integration_hub.models import StagedRecord


def content_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=_json_default).encode()).hexdigest()


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def json_compatible(payload: dict) -> dict:
    return json.loads(json.dumps(payload, default=_json_default))


class RecordSink(Protocol):
    """Destination for pulled records. Swap this to publish to Kafka, S3, a warehouse, etc."""

    async def write(
        self, session: AsyncSession, connection_id: str, spec: StreamSpec, records: list[Record]
    ) -> int: ...


class StagingSink:
    """Default sink: upserts into `staged_records`, skipping unchanged rows."""

    async def write(
        self, session: AsyncSession, connection_id: str, spec: StreamSpec, records: list[Record]
    ) -> int:
        if not records:
            return 0
        ids = [r.external_id for r in records if r.external_id]
        existing = {
            row.external_id: row
            for row in (
                await session.execute(
                    select(StagedRecord).where(
                        StagedRecord.connection_id == connection_id,
                        StagedRecord.stream == spec.name,
                        StagedRecord.external_id.in_(ids),
                    )
                )
            ).scalars()
        }

        changed = 0
        for rec in records:
            if not rec.external_id:
                continue
            payload = json_compatible(rec.canonical)
            digest = content_hash(payload)
            row = existing.get(rec.external_id)
            if row and row.content_hash == digest and row.deleted == rec.deleted:
                continue
            if row is None:
                row = StagedRecord(
                    connection_id=connection_id,
                    stream=spec.name,
                    object_type=str(spec.object_type),
                    external_id=rec.external_id,
                )
                session.add(row)
            row.payload = payload
            row.raw = rec.raw
            row.content_hash = digest
            row.deleted = rec.deleted
            row.remote_updated_at = rec.updated_at
            changed += 1
        return changed

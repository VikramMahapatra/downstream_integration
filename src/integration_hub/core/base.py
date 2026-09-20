from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, AsyncIterator, Awaitable, Callable

from integration_hub.core.canonical import ObjectType
from integration_hub.core.mapping import ObjectMapping


class Capability(StrEnum):
    READ = "read"
    INCREMENTAL_READ = "incremental_read"
    WRITE = "write"
    UPSERT = "upsert"
    DELETE = "delete"
    WEBHOOK = "webhook"
    SCHEMA_DISCOVERY = "schema_discovery"


class WriteMode(StrEnum):
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(slots=True)
class StreamSpec:
    """A readable/writable collection exposed by a provider (a Zoho module, a HubSpot object...)."""

    name: str
    object_type: ObjectType
    capabilities: set[Capability]
    cursor_field: str | None = None
    primary_key: str = "id"
    dedupe_fields: list[str] = field(default_factory=list)

    def supports(self, cap: Capability) -> bool:
        return cap in self.capabilities


@dataclass(slots=True)
class Cursor:
    """Incremental checkpoint persisted between runs."""

    since: datetime | None = None
    page_token: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "since": self.since.isoformat() if self.since else None,
            "page_token": self.page_token,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict | None) -> "Cursor":
        data = data or {}
        raw = data.get("since")
        return cls(
            since=datetime.fromisoformat(raw) if raw else None,
            page_token=data.get("page_token"),
            extra=data.get("extra") or {},
        )


@dataclass(slots=True)
class Record:
    """A single record flowing through the pipeline."""

    external_id: str | None
    raw: dict[str, Any]
    canonical: dict[str, Any]
    updated_at: datetime | None = None
    deleted: bool = False


@dataclass(slots=True)
class ReadPage:
    records: list[Record]
    cursor: Cursor
    has_more: bool


@dataclass(slots=True)
class WriteOutcome:
    external_id: str | None
    status: str  # created | updated | skipped | failed
    idempotency_key: str | None = None
    error: str | None = None
    code: str | None = None


@dataclass(slots=True)
class WriteResult:
    outcomes: list[WriteOutcome]

    @property
    def succeeded(self) -> int:
        return sum(1 for o in self.outcomes if o.status in ("created", "updated"))

    @property
    def failed(self) -> int:
        return sum(1 for o in self.outcomes if o.status == "failed")


@dataclass(slots=True)
class WebhookEvent:
    provider: str
    connection_id: str | None
    stream: str
    external_id: str
    operation: str  # create | update | delete
    occurred_at: datetime | None
    raw: dict[str, Any]
    dedupe_key: str


@dataclass
class ConnectionContext:
    """Everything a connector needs to act on behalf of one tenant connection."""

    connection_id: str
    tenant_id: str
    provider: str
    config: dict[str, Any]
    secrets: dict[str, Any]
    # Persists rotated credentials (refresh-token rotation, new access token, api domain...)
    save_secrets: Callable[[dict[str, Any]], Awaitable[None]]

    def require(self, key: str) -> Any:
        from integration_hub.core.errors import ConfigurationError

        value = self.config.get(key) or self.secrets.get(key)
        if value in (None, ""):
            raise ConfigurationError(f"'{key}' missing on connection {self.connection_id}")
        return value


class Connector(abc.ABC):
    """Contract every integration implements.

    Adding a new SaaS system means: subclass this, declare streams + mappings,
    register it, and it immediately gets scheduling, retries, checkpointing,
    encryption, webhooks and the REST API for free.
    """

    provider_key: str = ""
    display_name: str = ""
    capabilities: set[Capability] = set()

    def __init__(self, ctx: ConnectionContext):
        self.ctx = ctx

    async def aclose(self) -> None:  # pragma: no cover - optional hook
        return None

    # --- discovery ---
    @abc.abstractmethod
    def streams(self) -> list[StreamSpec]: ...

    def stream(self, name: str) -> StreamSpec:
        for s in self.streams():
            if s.name == name:
                return s
        from integration_hub.core.errors import ConfigurationError

        raise ConfigurationError(f"Unknown stream '{name}' for provider '{self.provider_key}'")

    @abc.abstractmethod
    def mapping(self, stream: str) -> ObjectMapping: ...

    # --- health ---
    @abc.abstractmethod
    async def check_connection(self) -> dict[str, Any]: ...

    # --- inbound ---
    @abc.abstractmethod
    def read(self, stream: str, cursor: Cursor, *, page_size: int) -> AsyncIterator[ReadPage]: ...

    # --- outbound ---
    @abc.abstractmethod
    async def write(self, stream: str, records: list[dict], *, mode: WriteMode) -> WriteResult: ...

    # --- webhooks (optional) ---
    async def register_webhook(self, streams: list[str], callback_url: str) -> dict[str, Any]:
        raise NotImplementedError

    async def unregister_webhook(self, subscription: dict[str, Any]) -> None:
        raise NotImplementedError

    def parse_webhook(self, headers: dict[str, str], body: dict) -> list[WebhookEvent]:
        raise NotImplementedError

    # --- optional schema discovery ---
    async def describe(self, stream: str) -> list[dict[str, Any]]:
        raise NotImplementedError

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from integration_hub.core.base import WriteMode


class ProviderOut(BaseModel):
    provider: str
    display_name: str
    capabilities: list[str]
    oauth: bool


class ConnectionCreate(BaseModel):
    tenant_id: str
    provider: str
    name: str
    config: dict[str, Any] = Field(default_factory=dict)
    # Only for providers authenticated with static credentials (API key, refresh token paste-in).
    secrets: dict[str, Any] = Field(default_factory=dict)


class ConnectionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    tenant_id: str
    provider: str
    name: str
    status: str
    config: dict[str, Any]
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class StreamConfigIn(BaseModel):
    stream: str
    enabled: bool = True
    schedule_seconds: int = 900
    field_overrides: dict[str, Any] = Field(default_factory=dict)


class StreamStateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    stream: str
    enabled: bool
    schedule_seconds: int
    cursor: dict[str, Any]
    last_run_at: datetime | None
    next_run_at: datetime | None


class SyncTriggerIn(BaseModel):
    stream: str
    full_refresh: bool = False
    max_pages: int | None = None


class SyncRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    connection_id: str
    stream: str
    direction: str
    status: str
    trigger: str
    started_at: datetime
    finished_at: datetime | None
    records_read: int
    records_written: int
    records_failed: int
    error: str | None


class PushIn(BaseModel):
    stream: str
    mode: WriteMode = WriteMode.UPSERT
    records: list[dict[str, Any]]
    # When true the records are queued in the outbox instead of being sent inline.
    queue: bool = True


class PushOut(BaseModel):
    queued: int = 0
    succeeded: int = 0
    failed: int = 0
    results: list[dict[str, Any]] = Field(default_factory=list)


class AuthorizeOut(BaseModel):
    authorize_url: str
    state: str


class WebhookRegisterIn(BaseModel):
    streams: list[str]
    callback_url: str | None = None


class StagedRecordOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    external_id: str
    object_type: str
    content_hash: str
    payload: dict[str, Any]
    deleted: bool
    remote_updated_at: datetime | None
    ingested_at: datetime

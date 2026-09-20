from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _uuid() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSON, list[str]: JSON}


class ConnectionStatus(StrEnum):
    PENDING_AUTH = "pending_auth"
    ACTIVE = "active"
    ERROR = "error"
    DISABLED = "disabled"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class Direction(StrEnum):
    INBOUND = "inbound"  # pull from provider into the hub
    OUTBOUND = "outbound"  # push from the hub into the provider


class Connection(Base):
    """One authenticated link between a tenant and a provider account/org."""

    __tablename__ = "connections"
    __table_args__ = (UniqueConstraint("tenant_id", "provider", "name", name="uq_conn_tenant_provider_name"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), default=ConnectionStatus.PENDING_AUTH)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    secrets_encrypted: Mapped[str | None] = mapped_column(Text, default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    streams: Mapped[list["StreamState"]] = relationship(
        back_populates="connection", cascade="all, delete-orphan", lazy="selectin"
    )


class StreamState(Base):
    """Per-stream sync configuration + incremental checkpoint."""

    __tablename__ = "stream_states"
    __table_args__ = (UniqueConstraint("connection_id", "stream", name="uq_stream_per_connection"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    stream: Mapped[str] = mapped_column(String(64))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    schedule_seconds: Mapped[int] = mapped_column(Integer, default=900)
    cursor: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    field_overrides: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)

    connection: Mapped[Connection] = relationship(back_populates="streams")


class SyncRun(Base):
    __tablename__ = "sync_runs"
    __table_args__ = (Index("ix_runs_conn_started", "connection_id", "started_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    stream: Mapped[str] = mapped_column(String(64))
    direction: Mapped[str] = mapped_column(String(16), default=Direction.INBOUND)
    status: Mapped[str] = mapped_column(String(16), default=RunStatus.QUEUED)
    trigger: Mapped[str] = mapped_column(String(32), default="schedule")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    records_read: Mapped[int] = mapped_column(Integer, default=0)
    records_written: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)
    cursor_before: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    cursor_after: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error: Mapped[str | None] = mapped_column(Text, default=None)


class RecordLink(Base):
    """Maps a hub-side entity to its identifier in a provider, enabling idempotent upserts."""

    __tablename__ = "record_links"
    __table_args__ = (
        UniqueConstraint("connection_id", "stream", "external_id", name="uq_link_external"),
        Index("ix_link_local", "connection_id", "stream", "local_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    stream: Mapped[str] = mapped_column(String(64))
    local_id: Mapped[str] = mapped_column(String(128))
    external_id: Mapped[str] = mapped_column(String(128))
    remote_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    remote_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class StagedRecord(Base):
    """Landing zone for pulled data. Downstream consumers read from here."""

    __tablename__ = "staged_records"
    __table_args__ = (UniqueConstraint("connection_id", "stream", "external_id", name="uq_staged_record"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    stream: Mapped[str] = mapped_column(String(64))
    object_type: Mapped[str] = mapped_column(String(32))
    external_id: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    content_hash: Mapped[str] = mapped_column(String(64))
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    remote_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class OutboxItem(Base):
    """Queued outbound change. Guarantees at-least-once delivery to the provider."""

    __tablename__ = "outbox_items"
    __table_args__ = (
        UniqueConstraint("connection_id", "idempotency_key", name="uq_outbox_idempotency"),
        Index("ix_outbox_pending", "status", "next_attempt_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    stream: Mapped[str] = mapped_column(String(64))
    operation: Mapped[str] = mapped_column(String(16))  # insert|update|upsert|delete
    idempotency_key: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    external_id: Mapped[str | None] = mapped_column(String(128), default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class WebhookDelivery(Base):
    """Raw inbound notification, stored before processing for replay + dedupe."""

    __tablename__ = "webhook_deliveries"
    __table_args__ = (UniqueConstraint("provider", "dedupe_key", name="uq_webhook_dedupe"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    connection_id: Mapped[str | None] = mapped_column(String(36), index=True, default=None)
    dedupe_key: Mapped[str] = mapped_column(String(200))
    stream: Mapped[str | None] = mapped_column(String(64), default=None)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    processed: Mapped[bool] = mapped_column(Boolean, default=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WebhookSubscription(Base):
    __tablename__ = "webhook_subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str] = mapped_column(ForeignKey("connections.id", ondelete="CASCADE"), index=True)
    channel_id: Mapped[str] = mapped_column(String(64))
    streams: Mapped[list[str]] = mapped_column(JSON, default=list)
    callback_url: Mapped[str] = mapped_column(String(512))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None, index=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    connection_id: Mapped[str | None] = mapped_column(String(36), index=True, default=None)
    stream: Mapped[str | None] = mapped_column(String(64), default=None)
    direction: Mapped[str] = mapped_column(String(16), default=Direction.OUTBOUND)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class OAuthState(Base):
    """Short-lived CSRF state for the authorization-code flow."""

    __tablename__ = "oauth_states"

    state: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(64))
    tenant_id: Mapped[str] = mapped_column(String(64))
    connection_id: Mapped[str | None] = mapped_column(String(36), default=None)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

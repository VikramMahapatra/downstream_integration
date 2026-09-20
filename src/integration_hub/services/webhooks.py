from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.core.base import Cursor, WebhookEvent
from integration_hub.db import session_scope
from integration_hub.logging_config import get_logger
from integration_hub.models import Connection, WebhookDelivery
from integration_hub.services.connections import build_connector
from integration_hub.services.sink import RecordSink, StagingSink

log = get_logger(__name__)


async def record_delivery(session: AsyncSession, event: WebhookEvent) -> bool:
    """Persist the notification. Returns False when it is a duplicate."""
    exists = (
        await session.execute(
            select(WebhookDelivery.id).where(
                WebhookDelivery.provider == event.provider,
                WebhookDelivery.dedupe_key == event.dedupe_key,
            )
        )
    ).scalar_one_or_none()
    if exists:
        return False
    session.add(
        WebhookDelivery(
            provider=event.provider,
            connection_id=event.connection_id,
            dedupe_key=event.dedupe_key,
            stream=event.stream,
            payload=event.raw,
        )
    )
    return True


async def process_event(event: WebhookEvent, sink: RecordSink | None = None) -> None:
    """Fetch the changed record by id and land it, so webhook and polling paths converge."""
    sink = sink or StagingSink()
    if not event.connection_id:
        log.warning("webhook without connection binding", extra={"provider": event.provider})
        return

    async with session_scope() as session:
        conn = await session.get(Connection, event.connection_id)
        if conn is None:
            return
        connector = build_connector(conn)

    try:
        spec = connector.stream(event.stream)
        cursor = Cursor(extra={"ids": [event.external_id], "operation": event.operation})
        async for page in connector.read(event.stream, cursor, page_size=1):
            async with session_scope() as session:
                await sink.write(session, event.connection_id, spec, page.records)
    finally:
        await connector.aclose()

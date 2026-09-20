from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.core.base import WriteMode
from integration_hub.core.errors import AuthError, IntegrationError
from integration_hub.db import session_scope
from integration_hub.logging_config import get_logger
from integration_hub.models import DeadLetter, Direction, OutboxItem, RecordLink, utcnow
from integration_hub.services.connections import build_connector, get_connection

log = get_logger(__name__)

MAX_ATTEMPTS = 6
BATCH_SIZE = 100


def make_idempotency_key(stream: str, operation: str, payload: dict, local_id: str | None) -> str:
    if local_id:
        return f"{stream}:{operation}:{local_id}"
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:32]
    return f"{stream}:{operation}:{digest}"


async def enqueue(
    session: AsyncSession,
    *,
    connection_id: str,
    stream: str,
    operation: WriteMode,
    payload: dict,
    local_id: str | None = None,
) -> OutboxItem:
    """Queue an outbound change. Re-enqueuing the same logical change is a no-op."""
    key = make_idempotency_key(stream, str(operation), payload, local_id)
    existing = (
        await session.execute(
            select(OutboxItem).where(
                OutboxItem.connection_id == connection_id, OutboxItem.idempotency_key == key
            )
        )
    ).scalar_one_or_none()
    if existing and existing.status == "pending":
        existing.payload = payload
        return existing

    item = OutboxItem(
        connection_id=connection_id,
        stream=stream,
        operation=str(operation),
        idempotency_key=key if not existing else f"{key}:{utcnow().timestamp():.0f}",
        payload=payload,
    )
    session.add(item)
    await session.flush()
    return item


class OutboxDispatcher:
    """Drains queued outbound changes, batching per (connection, stream, operation)."""

    async def dispatch_pending(self, limit: int = 500) -> dict[str, int]:
        async with session_scope() as session:
            items = list(
                (
                    await session.execute(
                        select(OutboxItem)
                        .where(OutboxItem.status == "pending", OutboxItem.next_attempt_at <= utcnow())
                        .order_by(OutboxItem.created_at)
                        .limit(limit)
                    )
                ).scalars()
            )
            for item in items:
                item.status = "in_flight"
            groups: dict[tuple[str, str, str], list[OutboxItem]] = defaultdict(list)
            for item in items:
                groups[(item.connection_id, item.stream, item.operation)].append(item)
            snapshot = {k: [(i.id, i.payload) for i in v] for k, v in groups.items()}

        stats = {"sent": 0, "failed": 0}
        for (connection_id, stream, operation), entries in snapshot.items():
            for chunk_start in range(0, len(entries), BATCH_SIZE):
                chunk = entries[chunk_start : chunk_start + BATCH_SIZE]
                result = await self._send_batch(connection_id, stream, operation, chunk)
                stats["sent"] += result[0]
                stats["failed"] += result[1]
        return stats

    async def _send_batch(
        self, connection_id: str, stream: str, operation: str, entries: list[tuple[str, dict]]
    ) -> tuple[int, int]:
        async with session_scope() as session:
            conn = await get_connection(session, connection_id)
            connector = build_connector(conn)

        payloads = [p for _, p in entries]
        try:
            result = await connector.write(stream, payloads, mode=WriteMode(operation))
            outcomes = result.outcomes
        except (AuthError, IntegrationError) as exc:
            await self._reschedule_all(entries, str(exc), permanent=not exc.retryable)
            return 0, len(entries)
        finally:
            await connector.aclose()

        sent = failed = 0
        async with session_scope() as session:
            for (item_id, payload), outcome in zip(entries, outcomes):
                item = await session.get(OutboxItem, item_id)
                if item is None:
                    continue
                item.attempts += 1
                if outcome.status == "failed":
                    failed += 1
                    item.last_error = outcome.error
                    if item.attempts >= MAX_ATTEMPTS:
                        item.status = "dead"
                        session.add(
                            DeadLetter(
                                connection_id=connection_id,
                                stream=stream,
                                direction=Direction.OUTBOUND,
                                payload=payload,
                                error=outcome.error or "unknown",
                            )
                        )
                    else:
                        item.status = "pending"
                        item.next_attempt_at = utcnow() + timedelta(seconds=30 * 2**item.attempts)
                else:
                    sent += 1
                    item.status = "done"
                    item.external_id = outcome.external_id
                    item.last_error = None
                    await self._link(session, connection_id, stream, item, outcome.external_id)
        return sent, failed

    @staticmethod
    async def _link(
        session: AsyncSession, connection_id: str, stream: str, item: OutboxItem, external_id: str | None
    ) -> None:
        if not external_id:
            return
        local_id = item.idempotency_key.split(":", 2)[-1]
        link = (
            await session.execute(
                select(RecordLink).where(
                    RecordLink.connection_id == connection_id,
                    RecordLink.stream == stream,
                    RecordLink.external_id == external_id,
                )
            )
        ).scalar_one_or_none()
        if link is None:
            session.add(
                RecordLink(
                    connection_id=connection_id, stream=stream, local_id=local_id, external_id=external_id
                )
            )

    @staticmethod
    async def _reschedule_all(entries: list[tuple[str, dict]], error: str, *, permanent: bool) -> None:
        async with session_scope() as session:
            for item_id, _ in entries:
                item = await session.get(OutboxItem, item_id)
                if item is None:
                    continue
                item.attempts += 1
                item.last_error = error
                if permanent or item.attempts >= MAX_ATTEMPTS:
                    item.status = "dead"
                else:
                    item.status = "pending"
                    item.next_attempt_at = utcnow() + timedelta(seconds=30 * 2**item.attempts)

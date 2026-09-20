from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from integration_hub.config import get_settings
from integration_hub.db import session_scope
from integration_hub.logging_config import get_logger
from integration_hub.models import Connection, ConnectionStatus, StreamState, utcnow
from integration_hub.services.outbox import OutboxDispatcher
from integration_hub.services.sync import SyncEngine

log = get_logger(__name__)


class Worker:
    """Single-process scheduler. Replace with Celery/arq beat when you scale out;
    the unit of work (`SyncEngine.run_stream`) stays identical."""

    def __init__(self, max_concurrent: int = 4):
        self.settings = get_settings()
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.engine = SyncEngine()
        self.dispatcher = OutboxDispatcher()
        self._sem = asyncio.Semaphore(max_concurrent)

    def start(self) -> None:
        interval = self.settings.scheduler_poll_seconds
        self.scheduler.add_job(self.tick_inbound, "interval", seconds=interval, max_instances=1)
        self.scheduler.add_job(self.tick_outbound, "interval", seconds=interval, max_instances=1)
        self.scheduler.start()
        log.info("scheduler started", extra={"poll_seconds": interval})

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def tick_inbound(self) -> None:
        async with session_scope() as session:
            rows = list(
                (
                    await session.execute(
                        select(StreamState.connection_id, StreamState.stream)
                        .join(Connection, Connection.id == StreamState.connection_id)
                        .where(
                            StreamState.enabled.is_(True),
                            StreamState.next_run_at <= utcnow(),
                            Connection.status == ConnectionStatus.ACTIVE,
                        )
                        .limit(50)
                    )
                ).all()
            )
        if rows:
            await asyncio.gather(*(self._run_one(cid, stream) for cid, stream in rows))

    async def _run_one(self, connection_id: str, stream: str) -> None:
        async with self._sem:
            try:
                await self.engine.run_stream(connection_id, stream, trigger="schedule")
            except Exception:
                log.exception("sync failed", extra={"connection_id": connection_id, "stream": stream})

    async def tick_outbound(self) -> None:
        try:
            await self.dispatcher.dispatch_pending()
        except Exception:
            log.exception("outbox dispatch failed")

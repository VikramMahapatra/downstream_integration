from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.config import get_settings
from integration_hub.core.base import Connector, Cursor
from integration_hub.core.errors import AuthError, IntegrationError
from integration_hub.db import session_scope
from integration_hub.logging_config import get_logger
from integration_hub.models import (
    Connection,
    ConnectionStatus,
    Direction,
    RunStatus,
    StreamState,
    SyncRun,
    utcnow,
)
from integration_hub.services.connections import build_connector, get_connection
from integration_hub.services.sink import RecordSink, StagingSink

log = get_logger(__name__)


class SyncEngine:
    """Runs one (connection, stream) pull: read pages, map, sink, checkpoint.

    The checkpoint is advanced only after a page is durably persisted, so a crash
    replays at most one page (at-least-once delivery, deduped by content hash).
    """

    def __init__(self, sink: RecordSink | None = None):
        self.sink = sink or StagingSink()
        self.settings = get_settings()

    async def run_stream(
        self,
        connection_id: str,
        stream: str,
        *,
        trigger: str = "manual",
        full_refresh: bool = False,
        max_pages: int | None = None,
    ) -> SyncRun:
        async with session_scope() as session:
            conn = await get_connection(session, connection_id)
            state = await self._get_state(session, connection_id, stream)
            cursor = Cursor() if full_refresh else Cursor.from_dict(state.cursor)
            run = SyncRun(
                connection_id=connection_id,
                stream=stream,
                direction=Direction.INBOUND,
                status=RunStatus.RUNNING,
                trigger=trigger,
                cursor_before=cursor.to_dict(),
            )
            session.add(run)
            await session.flush()
            run_id = run.id
            connector = build_connector(conn)

        try:
            read, written, final_cursor = await self._pump(connector, stream, cursor, max_pages)
            status = RunStatus.SUCCESS
            error = None
        except AuthError as exc:
            read = written = 0
            final_cursor = cursor
            status, error = RunStatus.FAILED, f"auth: {exc.message}"
            await self._mark_connection_error(connection_id, error, disable=True)
        except IntegrationError as exc:
            read = written = 0
            final_cursor = cursor
            status, error = RunStatus.FAILED, exc.message
            await self._mark_connection_error(connection_id, error)
        finally:
            await connector.aclose()

        async with session_scope() as session:
            run = await session.get(SyncRun, run_id)
            assert run is not None
            run.status = status
            run.error = error
            run.records_read = read
            run.records_written = written
            run.cursor_after = final_cursor.to_dict()
            run.finished_at = utcnow()

            state = await self._get_state(session, connection_id, stream)
            if status == RunStatus.SUCCESS:
                state.cursor = final_cursor.to_dict()
            state.last_run_at = utcnow()
            state.next_run_at = utcnow() + timedelta(seconds=state.schedule_seconds)
            return run

    async def _pump(
        self, connector: Connector, stream: str, cursor: Cursor, max_pages: int | None
    ) -> tuple[int, int, Cursor]:
        spec = connector.stream(stream)
        read = written = pages = 0
        latest = cursor

        async for page in connector.read(stream, cursor, page_size=self.settings.default_page_size):
            read += len(page.records)
            async with session_scope() as session:
                written += await self.sink.write(session, connector.ctx.connection_id, spec, page.records)
                await self._persist_cursor(session, connector.ctx.connection_id, stream, page.cursor)
            latest = page.cursor
            pages += 1
            if max_pages and pages >= max_pages:
                break

        # Once the backlog is drained, the next run resumes from the newest seen change.
        latest = Cursor(since=latest.since or datetime.now(timezone.utc), page_token=None, extra=latest.extra)
        return read, written, latest

    async def _persist_cursor(
        self, session: AsyncSession, connection_id: str, stream: str, cursor: Cursor
    ) -> None:
        state = await self._get_state(session, connection_id, stream)
        state.cursor = cursor.to_dict()

    @staticmethod
    async def _get_state(session: AsyncSession, connection_id: str, stream: str) -> StreamState:
        state = (
            await session.execute(
                select(StreamState).where(
                    StreamState.connection_id == connection_id, StreamState.stream == stream
                )
            )
        ).scalar_one_or_none()
        if state is None:
            state = StreamState(connection_id=connection_id, stream=stream)
            session.add(state)
            await session.flush()
        return state

    @staticmethod
    async def _mark_connection_error(connection_id: str, error: str, *, disable: bool = False) -> None:
        async with session_scope() as session:
            conn = await session.get(Connection, connection_id)
            if conn is None:
                return
            conn.last_error = error
            conn.status = ConnectionStatus.PENDING_AUTH if disable else ConnectionStatus.ERROR

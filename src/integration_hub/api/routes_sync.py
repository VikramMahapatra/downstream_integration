from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.api.deps import require_api_key
from integration_hub.api.schemas import PushIn, PushOut, SyncRunOut, SyncTriggerIn
from integration_hub.core.errors import IntegrationError
from integration_hub.db import get_session
from integration_hub.models import SyncRun
from integration_hub.services.connections import build_connector, get_connection
from integration_hub.services.outbox import OutboxDispatcher, enqueue
from integration_hub.services.sync import SyncEngine

router = APIRouter(prefix="/v1/connections", tags=["sync"], dependencies=[Depends(require_api_key)])


@router.post("/{connection_id}/sync", response_model=SyncRunOut)
async def trigger_sync(connection_id: str, payload: SyncTriggerIn) -> SyncRun:
    try:
        return await SyncEngine().run_stream(
            connection_id,
            payload.stream,
            trigger="manual",
            full_refresh=payload.full_refresh,
            max_pages=payload.max_pages,
        )
    except IntegrationError as exc:
        raise HTTPException(status_code=400, detail={"error": exc.message, "details": exc.details}) from exc


@router.get("/{connection_id}/runs", response_model=list[SyncRunOut])
async def list_runs(
    connection_id: str,
    limit: int = Query(50, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[SyncRun]:
    stmt = (
        select(SyncRun)
        .where(SyncRun.connection_id == connection_id)
        .order_by(SyncRun.started_at.desc())
        .limit(limit)
    )
    return list((await session.execute(stmt)).scalars())


@router.post("/{connection_id}/push", response_model=PushOut)
async def push(
    connection_id: str, payload: PushIn, session: AsyncSession = Depends(get_session)
) -> PushOut:
    """Send canonical records to the provider.

    `queue=true` (default) is the durable path: records land in the outbox and are
    delivered with retries. `queue=false` writes synchronously for interactive use.
    """
    conn = await get_connection(session, connection_id)

    if payload.queue:
        for record in payload.records:
            await enqueue(
                session,
                connection_id=connection_id,
                stream=payload.stream,
                operation=payload.mode,
                payload=record,
                local_id=record.get("local_id") or record.get("external_id"),
            )
        return PushOut(queued=len(payload.records))

    connector = build_connector(conn)
    try:
        result = await connector.write(payload.stream, payload.records, mode=payload.mode)
    except IntegrationError as exc:
        raise HTTPException(status_code=400, detail={"error": exc.message, "details": exc.details}) from exc
    finally:
        await connector.aclose()

    return PushOut(
        succeeded=result.succeeded,
        failed=result.failed,
        results=[asdict(o) for o in result.outcomes],
    )


@router.post("/{connection_id}/outbox/flush", response_model=dict)
async def flush_outbox(connection_id: str) -> dict:
    return await OutboxDispatcher().dispatch_pending()

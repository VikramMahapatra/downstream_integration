from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.api.deps import require_api_key
from integration_hub.api.schemas import (
    ConnectionCreate,
    ConnectionOut,
    ProviderOut,
    StagedRecordOut,
    StreamConfigIn,
    StreamStateOut,
)
from integration_hub.core.crypto import encrypt_secrets
from integration_hub.core.errors import IntegrationError
from integration_hub.core.registry import get_connector_class, list_providers
from integration_hub.db import get_session
from integration_hub.models import Connection, ConnectionStatus, StagedRecord, StreamState
from integration_hub.services.connections import build_connector, get_connection, list_connections

router = APIRouter(prefix="/v1", tags=["connections"], dependencies=[Depends(require_api_key)])


@router.get("/providers", response_model=list[ProviderOut])
async def providers() -> list[dict]:
    return list_providers()


@router.get("/providers/{provider}/streams")
async def provider_streams(provider: str) -> list[dict]:
    cls = get_connector_class(provider)
    dummy = cls.__new__(cls)  # streams() is static metadata; no credentials needed
    return [
        {
            "name": s.name,
            "object_type": str(s.object_type),
            "capabilities": sorted(str(c) for c in s.capabilities),
            "cursor_field": s.cursor_field,
            "dedupe_fields": s.dedupe_fields,
        }
        for s in dummy.streams()
    ]


@router.post("/connections", response_model=ConnectionOut, status_code=201)
async def create_connection(
    payload: ConnectionCreate, session: AsyncSession = Depends(get_session)
) -> Connection:
    get_connector_class(payload.provider)  # validates the provider exists
    conn = Connection(
        tenant_id=payload.tenant_id,
        provider=payload.provider,
        name=payload.name,
        config=payload.config,
        status=ConnectionStatus.ACTIVE if payload.secrets else ConnectionStatus.PENDING_AUTH,
        secrets_encrypted=encrypt_secrets(payload.secrets) if payload.secrets else None,
    )
    session.add(conn)
    await session.flush()
    return conn


@router.get("/connections", response_model=list[ConnectionOut])
async def get_connections(
    tenant_id: str | None = None, session: AsyncSession = Depends(get_session)
) -> list[Connection]:
    return await list_connections(session, tenant_id)


@router.get("/connections/{connection_id}", response_model=ConnectionOut)
async def read_connection(connection_id: str, session: AsyncSession = Depends(get_session)) -> Connection:
    return await get_connection(session, connection_id)


@router.delete("/connections/{connection_id}", status_code=204)
async def remove_connection(connection_id: str, session: AsyncSession = Depends(get_session)) -> None:
    await session.execute(delete(Connection).where(Connection.id == connection_id))


@router.post("/connections/{connection_id}/test")
async def test_connection(connection_id: str, session: AsyncSession = Depends(get_session)) -> dict:
    conn = await get_connection(session, connection_id)
    connector = build_connector(conn)
    try:
        result = await connector.check_connection()
        conn.status = ConnectionStatus.ACTIVE
        conn.last_error = None
        return result
    except IntegrationError as exc:
        conn.status = ConnectionStatus.ERROR
        conn.last_error = exc.message
        raise HTTPException(status_code=400, detail={"error": exc.message, "details": exc.details}) from exc
    finally:
        await connector.aclose()


@router.get("/connections/{connection_id}/schema/{stream}")
async def describe_stream(
    connection_id: str, stream: str, session: AsyncSession = Depends(get_session)
) -> list[dict]:
    conn = await get_connection(session, connection_id)
    connector = build_connector(conn)
    try:
        return await connector.describe(stream)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail="Provider does not support schema discovery") from exc
    finally:
        await connector.aclose()


@router.put("/connections/{connection_id}/streams", response_model=list[StreamStateOut])
async def configure_streams(
    connection_id: str, payload: list[StreamConfigIn], session: AsyncSession = Depends(get_session)
) -> list[StreamState]:
    conn = await get_connection(session, connection_id)
    connector = build_connector(conn)
    try:
        valid = {s.name for s in connector.streams()}
    finally:
        await connector.aclose()

    out: list[StreamState] = []
    for item in payload:
        if item.stream not in valid:
            raise HTTPException(status_code=400, detail=f"Unknown stream '{item.stream}'")
        state = (
            await session.execute(
                select(StreamState).where(
                    StreamState.connection_id == connection_id, StreamState.stream == item.stream
                )
            )
        ).scalar_one_or_none()
        if state is None:
            state = StreamState(connection_id=connection_id, stream=item.stream)
            session.add(state)
        state.enabled = item.enabled
        state.schedule_seconds = item.schedule_seconds
        state.field_overrides = item.field_overrides
        out.append(state)
    await session.flush()
    return out


@router.get("/connections/{connection_id}/streams", response_model=list[StreamStateOut])
async def get_streams(connection_id: str, session: AsyncSession = Depends(get_session)) -> list[StreamState]:
    return list(
        (
            await session.execute(select(StreamState).where(StreamState.connection_id == connection_id))
        ).scalars()
    )


@router.get("/connections/{connection_id}/records/{stream}", response_model=list[StagedRecordOut])
async def staged_records(
    connection_id: str,
    stream: str,
    limit: int = Query(100, le=1000),
    offset: int = 0,
    session: AsyncSession = Depends(get_session),
) -> list[StagedRecord]:
    stmt = (
        select(StagedRecord)
        .where(StagedRecord.connection_id == connection_id, StagedRecord.stream == stream)
        .order_by(StagedRecord.ingested_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return list((await session.execute(stmt)).scalars())

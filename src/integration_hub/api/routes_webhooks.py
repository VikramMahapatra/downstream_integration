from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.api.deps import require_api_key
from integration_hub.api.schemas import WebhookRegisterIn
from integration_hub.config import get_settings
from integration_hub.core.errors import IntegrationError
from integration_hub.db import get_session, session_scope
from integration_hub.models import Connection, WebhookSubscription
from integration_hub.services.connections import build_connector, get_connection
from integration_hub.services.webhooks import process_event, record_delivery

router = APIRouter(prefix="/v1", tags=["webhooks"])


@router.post(
    "/connections/{connection_id}/webhooks",
    dependencies=[Depends(require_api_key)],
)
async def register(
    connection_id: str, payload: WebhookRegisterIn, session: AsyncSession = Depends(get_session)
) -> dict:
    conn = await get_connection(session, connection_id)
    settings = get_settings()
    callback = payload.callback_url or (
        f"{settings.api_base_url}/v1/webhooks/{conn.provider}/{connection_id}"
    )
    connector = build_connector(conn)
    try:
        result = await connector.register_webhook(payload.streams, callback)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail="Provider does not support webhooks") from exc
    except IntegrationError as exc:
        raise HTTPException(status_code=400, detail={"error": exc.message, "details": exc.details}) from exc
    finally:
        await connector.aclose()

    expires = result.get("expires_at")
    session.add(
        WebhookSubscription(
            connection_id=connection_id,
            channel_id=str(result["channel_id"]),
            streams=payload.streams,
            callback_url=callback,
            expires_at=datetime.fromisoformat(expires) if expires else None,
            details=result.get("response") or {},
        )
    )
    return result


@router.get("/connections/{connection_id}/webhooks", dependencies=[Depends(require_api_key)])
async def list_subscriptions(
    connection_id: str, session: AsyncSession = Depends(get_session)
) -> list[dict]:
    rows = (
        await session.execute(
            select(WebhookSubscription).where(WebhookSubscription.connection_id == connection_id)
        )
    ).scalars()
    return [
        {
            "id": r.id,
            "channel_id": r.channel_id,
            "streams": r.streams,
            "callback_url": r.callback_url,
            "expires_at": r.expires_at,
        }
        for r in rows
    ]


@router.post("/webhooks/{provider}/{connection_id}", status_code=202)
async def receive(
    provider: str,
    connection_id: str,
    request: Request,
    background: BackgroundTasks,
) -> dict:
    """Public endpoint. Authentication is the provider-specific payload token/signature,
    verified inside `parse_webhook`; we ACK fast and process asynchronously."""
    body = await request.json()
    
    print(f"Received webhook for provider {provider}, connection {connection_id}: {body}")

    async with session_scope() as session:
        conn = await session.get(Connection, connection_id)
        
        print(
            "WEBHOOK CONNECTION CHECK:",
            {
                "connection_id": connection_id,
                "provider_from_url": provider,
                "connection_exists": conn is not None,
                "db_provider": conn.provider if conn else None,
            },
        )
        
        if conn is None or conn.provider != provider:
            raise HTTPException(status_code=404, detail="Unknown connection")
        connector = build_connector(conn)

    try:
        events = connector.parse_webhook(dict(request.headers), body)
    except IntegrationError as exc:
        raise HTTPException(status_code=401, detail=exc.message) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail="Provider does not support webhooks") from exc
    finally:
        await connector.aclose()

    accepted = 0
    async with session_scope() as session:
        for event in events:
            if await record_delivery(session, event):
                accepted += 1
                background.add_task(process_event, event)

    return {"received": len(events), "accepted": accepted, "at": datetime.now(timezone.utc).isoformat()}

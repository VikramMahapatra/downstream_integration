from __future__ import annotations

import secrets as pysecrets

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.api.deps import require_api_key
from integration_hub.api.schemas import AuthorizeOut
from integration_hub.config import get_settings
from integration_hub.core.crypto import decrypt_secrets, encrypt_secrets
from integration_hub.core.errors import AuthError
from integration_hub.db import get_session, session_scope
from integration_hub.models import Connection, ConnectionStatus, OAuthState
from integration_hub.providers.zoho_crm import auth as zoho_auth

router = APIRouter(prefix="/v1/oauth", tags=["oauth"])


@router.get("/{provider}/authorize", response_model=AuthorizeOut, dependencies=[Depends(require_api_key)])
async def authorize(
    provider: str,
    connection_id: str = Query(..., description="Connection to bind the credentials to"),
    session: AsyncSession = Depends(get_session),
) -> AuthorizeOut:
    if provider != "zoho_crm":
        raise HTTPException(status_code=404, detail=f"OAuth not implemented for '{provider}'")

    conn = await session.get(Connection, connection_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")

    state = pysecrets.token_urlsafe(32)
    session.add(
        OAuthState(
            state=state, provider=provider, tenant_id=conn.tenant_id, connection_id=connection_id, payload={}
        )
    )
    dc = conn.config.get("dc") or get_settings().zoho_default_dc
    return AuthorizeOut(authorize_url=zoho_auth.build_authorize_url(dc=dc, state=state), state=state)


@router.get("/zoho_crm/callback")
async def zoho_callback(
    code: str | None = None,
    state: str = "",
    error: str | None = None,
    location: str | None = None,
    accounts_server: str | None = Query(default=None, alias="accounts-server"),
) -> RedirectResponse:
    """Zoho redirects here with ?code, ?location (data centre) and ?accounts-server."""
    if error or not code:
        raise HTTPException(status_code=400, detail=f"Zoho authorization failed: {error or 'no code'}")

    async with session_scope() as session:
        row = await session.get(OAuthState, state)
        if row is None:
            raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")
        connection_id = row.connection_id
        await session.delete(row)

    if not connection_id:
        raise HTTPException(status_code=400, detail="OAuth state is not bound to a connection")

    dc = location or get_settings().zoho_default_dc
    try:
        token = await zoho_auth.exchange_code(code, dc=dc, accounts_server=accounts_server)
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=exc.message) from exc

    import time

    async with session_scope() as session:
        conn = await session.get(Connection, connection_id)
        if conn is None:
            raise HTTPException(status_code=404, detail="Connection not found")
        stored = decrypt_secrets(conn.secrets_encrypted)
        stored.update(
            {
                "access_token": token["access_token"],
                "refresh_token": token.get("refresh_token", stored.get("refresh_token")),
                "api_domain": token.get("api_domain"),
                "expires_at": time.time() + float(token.get("expires_in", 3600)) - 120,
            }
        )
        if not stored.get("refresh_token"):
            raise HTTPException(
                status_code=400,
                detail="Zoho did not return a refresh token. Re-authorize with access_type=offline "
                "and prompt=consent, or remove the app from the user's Connected Apps first.",
            )
        conn.secrets_encrypted = encrypt_secrets(stored)
        conn.config = {**(conn.config or {}), "dc": dc}
        conn.status = ConnectionStatus.ACTIVE
        conn.last_error = None

    return RedirectResponse(url=f"{get_settings().api_base_url}/v1/oauth/success?connection_id={connection_id}")


@router.get("/success")
async def success(connection_id: str) -> dict:
    return {"status": "connected", "connection_id": connection_id}

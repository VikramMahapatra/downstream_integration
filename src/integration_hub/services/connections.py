from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_hub.core.base import ConnectionContext, Connector
from integration_hub.core.crypto import decrypt_secrets, encrypt_secrets
from integration_hub.core.errors import ConfigurationError
from integration_hub.core.registry import get_connector_class
from integration_hub.db import session_scope
from integration_hub.models import Connection


async def get_connection(session: AsyncSession, connection_id: str) -> Connection:
    conn = await session.get(Connection, connection_id)
    if conn is None:
        raise ConfigurationError(f"Connection '{connection_id}' not found")
    return conn


async def list_connections(session: AsyncSession, tenant_id: str | None = None) -> list[Connection]:
    stmt = select(Connection)
    if tenant_id:
        stmt = stmt.where(Connection.tenant_id == tenant_id)
    return list((await session.execute(stmt.order_by(Connection.created_at.desc()))).scalars())


async def update_secrets(connection_id: str, secrets: dict) -> None:
    """Called by connectors when tokens rotate. Uses its own session to stay independent."""
    async with session_scope() as session:
        conn = await get_connection(session, connection_id)
        current = decrypt_secrets(conn.secrets_encrypted)
        current.update(secrets)
        conn.secrets_encrypted = encrypt_secrets(current)


def build_context(conn: Connection) -> ConnectionContext:
    async def _save(secrets: dict) -> None:
        await update_secrets(conn.id, secrets)

    return ConnectionContext(
        connection_id=conn.id,
        tenant_id=conn.tenant_id,
        provider=conn.provider,
        config=dict(conn.config or {}),
        secrets=decrypt_secrets(conn.secrets_encrypted),
        save_secrets=_save,
    )


def build_connector(conn: Connection) -> Connector:
    cls = get_connector_class(conn.provider)
    return cls(build_context(conn))


async def connector_for(session: AsyncSession, connection_id: str) -> Connector:
    return build_connector(await get_connection(session, connection_id))

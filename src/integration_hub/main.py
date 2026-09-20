from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from integration_hub.api import routes_connections, routes_oauth, routes_sync, routes_webhooks
from integration_hub.config import get_settings
from integration_hub.core.errors import AuthError, ConfigurationError, IntegrationError, RateLimitError
from integration_hub.core.registry import load_providers
from integration_hub.db import init_models
from integration_hub.logging_config import setup_logging
from integration_hub.worker import Worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    load_providers()
    settings = get_settings()
    if settings.app_env in ("local", "test"):
        await init_models()

    worker: Worker | None = None
    if settings.scheduler_enabled:
        worker = Worker()
        worker.start()
    try:
        yield
    finally:
        if worker:
            worker.shutdown()


app = FastAPI(
    title="Integration Hub",
    version="0.1.0",
    description="Pluggable pull/push integration backend. First provider: Zoho CRM.",
    lifespan=lifespan,
)

app.include_router(routes_connections.router)
app.include_router(routes_oauth.router)
app.include_router(routes_sync.router)
app.include_router(routes_webhooks.router)


@app.exception_handler(IntegrationError)
async def integration_error_handler(_: Request, exc: IntegrationError) -> JSONResponse:
    status = 400
    if isinstance(exc, AuthError):
        status = 401
    elif isinstance(exc, RateLimitError):
        status = 429
    elif isinstance(exc, ConfigurationError):
        status = 422
    return JSONResponse(
        status_code=status,
        content={"error": exc.message, "provider": exc.provider, "details": exc.details},
    )


@app.get("/health", tags=["ops"])
async def health() -> dict:
    return {"status": "ok"}

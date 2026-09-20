from __future__ import annotations

import argparse
import asyncio
import json

from integration_hub.core.registry import list_providers, load_providers
from integration_hub.db import init_models
from integration_hub.logging_config import setup_logging
from integration_hub.services.outbox import OutboxDispatcher
from integration_hub.services.sync import SyncEngine


def main() -> None:
    setup_logging()
    load_providers()

    parser = argparse.ArgumentParser(prog="ihub", description="Integration Hub operations CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create tables (dev only; use Alembic in production)")
    sub.add_parser("providers", help="List registered providers")

    run = sub.add_parser("sync", help="Run one stream now")
    run.add_argument("connection_id")
    run.add_argument("stream")
    run.add_argument("--full-refresh", action="store_true")
    run.add_argument("--max-pages", type=int, default=None)

    sub.add_parser("flush-outbox", help="Dispatch pending outbound records")

    args = parser.parse_args()
    asyncio.run(_dispatch(args))


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "init-db":
        await init_models()
        print("tables created")
    elif args.command == "providers":
        print(json.dumps(list_providers(), indent=2))
    elif args.command == "sync":
        run = await SyncEngine().run_stream(
            args.connection_id,
            args.stream,
            trigger="cli",
            full_refresh=args.full_refresh,
            max_pages=args.max_pages,
        )
        print(
            json.dumps(
                {
                    "status": run.status,
                    "read": run.records_read,
                    "written": run.records_written,
                    "error": run.error,
                },
                indent=2,
            )
        )
    elif args.command == "flush-outbox":
        print(json.dumps(await OutboxDispatcher().dispatch_pending(), indent=2))


if __name__ == "__main__":
    main()

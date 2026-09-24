"""FastAPI application and process entrypoint."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from controlplane.api import install_error_handlers, router
from controlplane.broker import Broker
from controlplane.config import Settings, parse_http_addr
from controlplane.service import Service
from controlplane.store import Database, Store

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        current = resolved if resolved is not None else Settings.from_env()
        db = Database(current.database_path)
        await db.connect()
        store = Store(db)
        await store.migrate()
        broker = await _retry(
            "nats",
            lambda: Broker.connect(current.nats_url, current.subject_prefix, current.stream_name),
        )
        service = Service(store, broker, current)
        app.state.service = service
        app.state.db = db
        outbox = asyncio.create_task(service.run_outbox(), name="outbox")
        progress = asyncio.create_task(service.run_progress(), name="progress")
        try:
            yield
        finally:
            outbox.cancel()
            progress.cancel()
            await asyncio.gather(outbox, progress, return_exceptions=True)
            await broker.close()
            await db.close()

    app = FastAPI(title="Tenant provisioning control plane", lifespan=lifespan)
    install_error_handlers(app)
    app.include_router(router)
    return app


async def _retry(label: str, factory):
    last: Exception | None = None
    for attempt in range(1, 31):
        try:
            return await factory()
        except Exception as exc:
            last = exc
            log.warning("waiting for %s attempt=%s err=%s", label, attempt, exc)
            await asyncio.sleep(1)
    if last is None:
        raise RuntimeError(f"{label} failed")
    raise last


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    host, port = parse_http_addr(settings.http_addr)
    uvicorn.run(create_app(settings), host=host, port=port, access_log=False)

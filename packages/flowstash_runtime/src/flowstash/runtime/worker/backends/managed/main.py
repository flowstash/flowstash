import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.observability.ingestion import AsyncManager
from .drain import ManagedTaskDrainController
from .http_entrypoint import router
from flowstash.runtime.wiring.runtime import initialize_runtime

logger = logging.getLogger(__name__)

_MANAGED_SHUTDOWN_WAIT_SECONDS = 8.0


async def _close_feed_backend() -> None:
    from flowstash.pipelines.records_feed import get_feed_backend

    backend = get_feed_backend()
    if backend and hasattr(backend, "close"):
        try:
            await backend.close()
        except Exception as e:
            logger.warning(f"Error closing feed backend: {e}")


async def _shutdown_managed_runtime(app: FastAPI, timeout_s: float) -> None:
    controller = app.state.managed_task_drain_controller

    await controller.start_draining("FastAPI shutdown")
    await controller.wait_for_idle(timeout_s)
    try:
        AsyncManager.get_instance().flush(timeout=timeout_s)
    except Exception as e:
        logger.warning(f"Error flushing observability during shutdown: {e}")
    await _close_feed_backend()


def _build_managed_lifespan(config: RuntimeConfig):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from flowstash.queue.backend import get_backend
        from .managed_consumer import sync_feed_consumers

        try:
            get_backend()
            logger.info("Runtime already initialized. Skipping re-initialization.")
        except RuntimeError:
            initialize_runtime(config)

        # Best-effort: sync feed consumer registrations on every service startup.
        await asyncio.to_thread(sync_feed_consumers, strict=False)

        yield
        await _shutdown_managed_runtime(app, _MANAGED_SHUTDOWN_WAIT_SECONDS)

    return lifespan


def create_app(config: RuntimeConfig) -> FastAPI:
    """
    Create the FastAPI application for the managed worker backend.
    """
    app = FastAPI(
        title="Managed Worker Runtime",
        lifespan=_build_managed_lifespan(config),
    )
    app.state.managed_task_drain_controller = ManagedTaskDrainController()

    # Include the task handling router
    app.include_router(router)

    @app.get("/health")
    async def health():
        """Health check endpoint for deployment verification."""
        return {"status": "ok"}

    return app


async def run_managed_http_server(config: RuntimeConfig) -> None:
    """Run the managed worker in HTTP mode for feed delivery callbacks."""
    app = create_app(config)
    port = int(os.getenv("PORT", "8080"))
    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    )
    await server.serve()

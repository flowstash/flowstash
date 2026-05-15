import logging
import os
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.observability.ingestion import AsyncManager
from flowstash.pipelines.consumer import get_registered_consumers
from .drain import ManagedTaskDrainController
from .http_entrypoint import router
from flowstash.runtime.wiring.runtime import initialize_runtime

logger = logging.getLogger(__name__)

_MANAGED_SHUTDOWN_WAIT_SECONDS = 8.0


async def _register_consumers_with_api(config: RuntimeConfig) -> None:
    """
    Push all locally registered @feed_consumer specs to the Managed Platform API.

    This allows the API's publish endpoint to know which consumer groups exist
    and what their batching parameters are, so it can buffer and kick correctly.
    """
    consumers = get_registered_consumers()
    if not consumers:
        return

    api_url = (
        os.getenv("FLOWSTASH_API_URL")
        or os.getenv("MANAGED_API_URL")
        or "https://api.flowstash.dev"
    ).rstrip("/")
    auth_token = os.getenv("MANAGED_AUTH_TOKEN", "")

    # Group by feed_id so we send one request per feed
    from collections import defaultdict

    by_feed: dict[str, list] = defaultdict(list)
    for spec in consumers:
        by_feed[spec.feed_id].append(spec)

    async with httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
        timeout=10.0,
    ) as client:
        for feed_id, specs in by_feed.items():
            payload = {
                "consumers": [
                    {
                        "group_name": s.subscription_name,
                        "batch": s.batch,
                        "max_batch_size": s.max_batch_size,
                        "max_delay_ms": s.max_delay_ms,
                    }
                    for s in specs
                ]
            }
            try:
                resp = await client.post(
                    f"{api_url}/v1/feed/{feed_id}/consumers/register",
                    json=payload,
                )
                resp.raise_for_status()
                logger.info(
                    f"Registered {len(specs)} consumer(s) for feed={feed_id} "
                    f"with Managed API"
                )
            except Exception as e:
                logger.warning(f"Failed to register consumers for feed={feed_id}: {e}")


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

        try:
            get_backend()
            logger.info("Runtime already initialized. Skipping re-initialization.")
        except RuntimeError:
            initialize_runtime(config)

        await _register_consumers_with_api(config)
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

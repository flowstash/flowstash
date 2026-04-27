import logging
import os

import httpx
from fastapi import FastAPI
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.pipelines.consumer import get_registered_consumers
from .http_entrypoint import router
from flowstash.runtime.wiring.runtime import initialize_runtime

logger = logging.getLogger(__name__)


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
                logger.warning(
                    f"Failed to register consumers for feed={feed_id}: {e}"
                )


def create_app(config: RuntimeConfig) -> FastAPI:
    """
    Create the FastAPI application for the managed worker backend.
    """
    app = FastAPI(title="Managed Worker Runtime")

    # Include the task handling router
    app.include_router(router)

    @app.get("/health")
    async def health():
        """Health check endpoint for deployment verification."""
        return {"status": "ok"}

    @app.on_event("startup")
    async def startup_event():
        from flowstash.queue.backend import get_backend
        try:
            get_backend()
            logger.info("Runtime already initialized. Skipping re-initialization.")
        except RuntimeError:
            # Wire up the runtime (backend, clients, etc.)
            initialize_runtime(config)

        # Register all @feed_consumer specs with the Managed API so publish
        # can buffer correctly and schedule kicks.
        await _register_consumers_with_api(config)

    @app.on_event("shutdown")
    async def shutdown_event():
        # Close the ManagedFeedBackend HTTP client to release connection pool.
        from flowstash.pipelines.records_feed import get_feed_backend
        backend = get_feed_backend()
        if backend and hasattr(backend, "close"):
            try:
                await backend.close()
            except Exception as e:
                logger.warning(f"Error closing feed backend: {e}")

    return app



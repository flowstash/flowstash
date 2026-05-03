"""
FastAPI application factory for webhook ingress.

Provides a convenience function to create a fully configured FastAPI app.
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional, Union

from fastapi import FastAPI
from flowstash.config.runtime_config import BackendType, RuntimeConfig
from .router import build_webhook_router
from ..wiring.runtime import Runtime, initialize_runtime

logger = logging.getLogger(__name__)


def _run_startup_task(task_name: str) -> None:
    """Find a registered task by name and submit it immediately."""
    from flowstash.queue.backend import _registered_task_wrappers

    for wrapper in _registered_task_wrappers:
        fn_name = wrapper.metadata.get("span_name") or getattr(
            wrapper.func, "__name__", None
        )
        if fn_name == task_name:
            logger.info(f"Executing startup task: {task_name}")
            wrapper.submit()
            return
    logger.warning(
        f"FLOWSTASH_STARTUP_TASK='{task_name}' not found in registered tasks."
    )


def _make_async_lifespan(rt: Runtime):
    """Build a FastAPI lifespan that runs APScheduler and an optional startup task."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        scheduler = None

        # --- Scheduled tasks ---
        enabled_raw = (
            os.environ.get("FLOWSTASH_ASYNC_SCHEDULED_ENABLE", "true").strip().lower()
        )
        scheduling_enabled = enabled_raw not in ("false", "0")

        if scheduling_enabled and getattr(rt.backend, "_scheduled_jobs", None):
            try:
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
                from apscheduler.triggers.cron import CronTrigger

                scheduler = AsyncIOScheduler()
                for job in rt.backend._scheduled_jobs:
                    wrapper = job["wrapper"]
                    args = job["args"]
                    kwargs = job["kwargs"]

                    def _make_trigger(w=wrapper, a=args, k=kwargs):
                        async def _trigger():
                            w.submit(*a, **k)

                        return _trigger

                    scheduler.add_job(
                        _make_trigger(),
                        CronTrigger.from_crontab(job["schedule"].cron),
                        id=job["scheduled_job_id"],
                        replace_existing=True,
                        name=job["func_name"],
                    )

                scheduler.start()
                logger.info(
                    f"Async scheduler started with {len(rt.backend._scheduled_jobs)} job(s)."
                )
            except ImportError:
                logger.warning(
                    "APScheduler not available — async scheduled tasks will not run."
                )

        # --- Startup task ---
        startup_task_name = os.environ.get("FLOWSTASH_STARTUP_TASK", "").strip()
        if startup_task_name:
            _run_startup_task(startup_task_name)

        logger.info("lifespan: startup complete")
        yield  # app serves requests here

        # --- Shutdown ---
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("Async scheduler stopped.")

        # Flush pending observability events before the process exits.
        # Two-step: (1) drain AsyncManager executor threads so events reach the store's
        # internal queue, then (2) drain the store queue so HTTP POSTs actually complete.
        try:
            from flowstash.observability.ingestion import AsyncManager
            from flowstash.observability.registry import flush_stores

            AsyncManager.get_instance().flush(timeout=10.0)
            flush_stores(timeout=10.0)
            logger.info("lifespan: shutdown flush complete")
        except Exception:
            pass

    return lifespan


@asynccontextmanager
async def _flush_lifespan(app: FastAPI):
    """Minimal lifespan that only flushes pending observability events on shutdown."""
    yield
    try:
        from flowstash.observability.ingestion import AsyncManager
        from flowstash.observability.registry import flush_stores

        AsyncManager.get_instance().flush(timeout=10.0)
        flush_stores(timeout=10.0)
    except Exception:
        pass


def create_fastapi_app(
    config: RuntimeConfig,
    auto_import: Optional[List[Union[str, Path]]] = None,
) -> FastAPI:
    """
    Create FastAPI app with webhook routes.

    For BackendType.ASYNC the app gains a lifespan that:
    - Starts APScheduler for cron-scheduled tasks (disable with FLOWSTASH_ASYNC_SCHEDULED_ENABLE=false|0)
    - Executes a named task once on startup when FLOWSTASH_STARTUP_TASK is set

    Args:
        config: RuntimeConfig with webhooks configuration
        auto_import: List of paths to auto-import modules from

    Returns:
        FastAPI application with webhook routes included
    """
    # Build runtime (imports modules, sets up backend)
    rt = initialize_runtime(config, auto_import=auto_import)

    # Attach a lifespan: async backend gets scheduler + flush; others get flush only
    if config.backend.type == BackendType.ASYNC:
        lifespan = _make_async_lifespan(rt)
    else:
        lifespan = _flush_lifespan

    # Create app
    app = FastAPI(title="Ingress API", lifespan=lifespan)

    # Include webhook router with configured prefix
    app.include_router(build_webhook_router(), prefix=config.webhooks.prefix)

    # Store runtime reference on app for access in middleware/dependencies if needed
    app.state.runtime = rt

    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("INFO:     %(message)s"))
        logger.addHandler(handler)

    # Include auto-imported routers
    if hasattr(rt, "imported_modules"):
        for mod in rt.imported_modules:
            if hasattr(mod, "router"):
                app.include_router(mod.router)
                mod_file = getattr(mod, "__file__", mod.__name__)
                logger.info(f"router automimpoted from [{mod_file}]")

    logger.info("Registered paths:")
    for route in app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        name = getattr(
            route,
            "name",
            (
                getattr(route, "endpoint", "").__name__
                if hasattr(route, "endpoint")
                else ""
            ),
        )
        if path:
            methods_str = ",".join(methods) if methods else "ANY"
            logger.info(f"- {methods_str} {path} [{name}]")

    return app

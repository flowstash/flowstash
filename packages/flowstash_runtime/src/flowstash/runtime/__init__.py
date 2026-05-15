"""
Framework Runtime - Pure glue around flowstash.

This package provides:
- Ingress API helpers (FastAPI router generation from webhook registry)
- Worker bootstrap (Dramatiq broker configuration with context middleware)
- Queue backends (Dramatiq implementation moved from flowstash)

Usage for Ingress API:
    from flowstash.runtime.ingress.app import create_fastapi_app
    from flowstash.config.loader import load_runtime_config

    config = load_runtime_config("/path/to/config")
    app = create_fastapi_app(config)

Usage for Worker:
    from flowstash.runtime.worker.entrypoint import configure_worker_process
    from flowstash.config.loader import load_runtime_config

    config = load_runtime_config("/path/to/config")
    runtime = configure_worker_process(config)
"""

# Main exports
from .wiring.runtime import Runtime, initialize_runtime, build_worker_runtime
from .ingress.app import create_fastapi_app
from .ingress.router import build_webhook_router
from .worker.runner import run_worker, build_worker_consumer
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("flowstash-runtime")
except PackageNotFoundError:
    __version__ = "0.0.0"


__all__ = [
    "Runtime",
    "initialize_runtime",
    "build_worker_runtime",
    "create_fastapi_app",
    "build_webhook_router",
    "run_worker",
    "build_worker_consumer",
]

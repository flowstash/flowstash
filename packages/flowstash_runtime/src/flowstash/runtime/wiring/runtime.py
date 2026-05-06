import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Optional, List, Union
import importlib
import importlib.metadata
import importlib.util
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from flowstash.config.runtime_config import RuntimeConfig, BackendType
from flowstash.queue.backend import set_backend, TaskBackend
from flowstash.runtime.worker.backends.dramatiq.dramatiq_backend import (
    DramatiqBackend,
    FrameworkContextMiddleware,
)


@dataclass
class Runtime:
    """Runtime container for backend and config references."""

    backend: TaskBackend
    config: RuntimeConfig
    imported_modules: List[Any] = field(default_factory=list)

    @property
    def broker(self):
        """Return the global Dramatiq broker."""
        import dramatiq

        return dramatiq.get_broker()


def _auto_import_path(path: Union[str, Path]) -> List[Any]:
    """
    Import modules from a file or directory path.

    If path is a directory:
    - If it contains __init__.py, import it as a module.
    - Otherwise, import all *.py files in it.
    If path is a .py file, import it.
    """
    p = Path(path)
    if not p.exists():
        return []

    modules = []
    if p.is_dir():
        # Using rglob to find all .py files in the directory recursively.
        # We ensure __init__.py files are imported first to initialize packages
        py_files = list(p.rglob("*.py"))

        # Sort so that __init__.py files come before other files in the same dir
        py_files.sort(
            key=lambda f: (len(f.parts), 0 if f.name == "__init__.py" else 1, f.name)
        )

        for f in py_files:
            mod = _import_module_from_path(f)
            if mod:
                modules.append(mod)

    elif p.is_file() and p.suffix == ".py":
        mod = _import_module_from_path(p)
        if mod:
            modules.append(mod)

    return modules


def _import_module_from_path(file_path: Path) -> Any:
    """
    Import a module from a file path dynamically, supporting relative imports.
    Standardizes on 'src/' as the import root when present.
    """
    import sys
    import importlib

    file_path = file_path.resolve()
    base_path = None
    module_parts = []

    # 1. Standardize on src/ as the import root (preferred strategy)
    # Search upwards for a directory named "src"
    current = file_path.parent if file_path.is_file() else file_path
    src_ancestor = None

    # Check current and all parents
    check_path = current
    while check_path != check_path.parent:
        if check_path.name == "src":
            src_ancestor = check_path
            break
        check_path = check_path.parent

    if src_ancestor:
        base_path = src_ancestor
        if file_path.is_file():
            rel = file_path.parent.relative_to(base_path)
            if file_path.name == "__init__.py":
                module_parts = list(rel.parts)
            else:
                module_parts = list(rel.parts) + [file_path.stem]
        else:
            rel = file_path.relative_to(base_path)
            module_parts = list(rel.parts)
    else:
        # 2. Fallback: Trace upwards to find the root of the package (directory without __init__.py)
        current_dir = file_path.parent if file_path.is_file() else file_path

        while (current_dir / "__init__.py").exists():
            current_dir = current_dir.parent

        base_path = current_dir

        if file_path.is_file():
            if file_path.name == "__init__.py":
                rel_path = file_path.parent.relative_to(base_path)
                module_parts = list(rel_path.parts)
            else:
                rel_path = file_path.parent.relative_to(base_path)
                module_parts = list(rel_path.parts) + [file_path.stem]
        else:
            if base_path == file_path:
                base_path = file_path.parent
            rel_path = file_path.relative_to(base_path)
            module_parts = list(rel_path.parts)

    module_name = ".".join(module_parts)

    # Inject package root into sys.path if not present
    base_path_str = str(base_path)
    if base_path_str not in sys.path:
        sys.path.insert(0, base_path_str)

    # Fail fast: do not swallow exceptions during import (Fix B)
    return importlib.import_module(module_name)


def _build_redis_url(config: RuntimeConfig) -> str:
    """Build Redis URL from config or environment."""
    if os.getenv("REDIS_URL"):
        return os.getenv("REDIS_URL")

    if config.backend.dramatiq and config.backend.dramatiq.redis_url:
        return config.backend.dramatiq.redis_url

    return "redis://localhost:6379/0"


def _get_flowstash_version() -> str:
    """Return the installed Flowstash version if package metadata is available."""
    for package_name in ("flowstash", "flowstash-runtime"):
        try:
            return importlib.metadata.version(package_name)
        except importlib.metadata.PackageNotFoundError:
            continue

    return "unknown"


def initialize_runtime(
    config: RuntimeConfig, auto_import: Optional[List[Union[str, Path]]] = None
) -> Runtime:
    """
    Initialize the framework runtime: configuration, clients, and task backend.

    This is the single entry point for setting up the producer side (backend submission)
    and registry initialization.
    """
    print(f"Flowstash version: {_get_flowstash_version()}")

    # 1. Init Observability (if present)
    if config.observability:
        from flowstash.observability.ingestion import set_observability_config

        set_observability_config(config.observability)

    # 2. Init Global Config Registry
    from flowstash.config.runtime_config import (
        RuntimeConfigRegistry,
        set_global_registry,
    )

    registry = RuntimeConfigRegistry(config)
    set_global_registry(registry)

    # 3. Handle auto-imports from paths
    imported_modules = []
    if auto_import:
        for path in auto_import:
            mods = _auto_import_path(path)
            if mods:
                imported_modules.extend(mods)

    # 3. Configure task backend based on config.backend.type
    backend: Optional[TaskBackend] = None
    if config.backend.type == BackendType.DRAMATIQ:
        redis_url = _build_redis_url(config)
        broker = RedisBroker(url=redis_url)
        dramatiq.set_broker(broker)

        # Dramatiq's built-in middlewares (like Prometheus) expect the process_boot
        # signal to be emitted to initialize their internal states (like prometheus Counters).
        # We must emit this manually here because we are initializing the broker programmatically
        # (not via dramatiq CLI), ensuring producers (API) also initialize metrics.
        broker.emit_after("process_boot")

        # Set flowstash.queue.backend
        backend = set_backend(DramatiqBackend())
    elif config.backend.type == BackendType.MANAGED:
        managed_api_url = (
            os.getenv("FLOWSTASH_API_URL")
            or os.getenv("MANAGED_API_URL")
            or "https://api.flowstash.dev"
        )
        managed_auth_token = os.getenv("MANAGED_AUTH_TOKEN")

        if managed_auth_token:
            from flowstash.queue.backends.managed_tasks import ManagedTasksBackend
            from flowstash.pipelines.backends.managed_feed import ManagedFeedBackend
            from flowstash.pipelines.records_feed import set_feed_backend

            backend = set_backend(
                ManagedTasksBackend(
                    api_url=managed_api_url,
                    auth_token=managed_auth_token,
                    service_url=os.getenv("MANAGED_SERVICE_URL"),
                )
            )

            set_feed_backend(
                ManagedFeedBackend(
                    api_url=managed_api_url,
                    auth_token=managed_auth_token,
                )
            )
        else:
            raise ValueError(
                "MANAGED backend requires MANAGED_AUTH_TOKEN environment variable to be set"
            )

    elif config.backend.type == BackendType.ASYNC:
        # Local asyncio development backend
        print(
            "Using asyncio development backend! This should not be used in production!"
        )

        from flowstash.queue.asyncio_backend import AsyncioBackend, AsyncioFeedBackend
        from flowstash.pipelines.records_feed import set_feed_backend

        backend = set_backend(AsyncioBackend())
        set_feed_backend(AsyncioFeedBackend())

        # In ASYNC mode, import worker_main AFTER the backend is set so that
        # register_schedule calls land on the correct backend instance.
        # Guard against re-entrant calls (worker_main may already be importing).
        import sys

        if "worker_main" not in sys.modules:
            try:
                import worker_main  # noqa: F401
            except ImportError as e:
                import logging

                logging.getLogger(__name__).warning(
                    f"Failed to auto-import 'worker_main' in ASYNC backend mode: {e}. "
                    "Tasks and consumers might not be registered if this process acts as an API server."
                )
            except Exception as e:
                import logging

                logging.getLogger(__name__).error(
                    f"Error while importing 'worker_main' in ASYNC backend mode: {e}. "
                    "Will continue without worker_main initialization."
                )
    else:
        raise ValueError(f"Unsupported backend: {config.backend.type}")

    if backend and hasattr(backend, "on_worker_init"):
        backend.on_worker_init()

    return Runtime(backend=backend, config=config, imported_modules=imported_modules)


def build_worker_runtime(
    config: RuntimeConfig, auto_import: Optional[List[Union[str, Path]]] = None
) -> Runtime:
    """
    Configure runtime for worker service.

    Deprecated: Use initialize_runtime(config) and then get a consumer.
    """
    # Import task modules is now out of scope for RuntimeConfig,
    # and they should be passed as auto_import paths or imported manually.

    return initialize_runtime(config, auto_import=auto_import)

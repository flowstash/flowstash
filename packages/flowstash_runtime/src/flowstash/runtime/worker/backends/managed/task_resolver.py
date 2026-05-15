"""
Task registry, function resolution, and callable dispatch.

Shared by the managed job consumer.
"""

import importlib
import inspect
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

_task_registry: Dict[str, Any] = {}


def register_task(task_id: str, func: Any) -> None:
    """Register a callable task by its ID."""
    _task_registry[task_id] = func
    logger.info(f"Registered task handler: {task_id}")


def get_task_registry() -> Dict[str, Any]:
    """Return the current task registry."""
    return _task_registry


def resolve_function(func_ref: str) -> Any:
    """
    Resolve 'module.path.function_name' to a callable.

    Checks the local registry first, then falls back to dynamic import.
    Raises ValueError if not found.
    """
    if func_ref in _task_registry:
        return _task_registry[func_ref]

    parts = func_ref.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid function reference: {func_ref}")

    module_path, func_name = parts
    try:
        module = importlib.import_module(module_path)
        return getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Cannot resolve function '{func_ref}': {e}")


async def _invoke_task_callable(func: Any, args: list, kwargs: dict) -> Any:
    """Dispatch a task callable regardless of its wrapper type."""
    if hasattr(func, "run"):
        return await func.run(*args, **kwargs)
    if hasattr(func, "func"):
        underlying = func.func
        if inspect.iscoroutinefunction(underlying):
            return await underlying(*args, **kwargs)
        return underlying(*args, **kwargs)
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    return func(*args, **kwargs)

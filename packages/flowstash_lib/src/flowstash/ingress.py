import functools
import inspect
from typing import Any, Callable, Optional, Dict, List, Union
from .context import integration_context, current_context
from .integration.state import State


class Ingress:
    """Namespace for ingress decorators."""

    def __init__(self):
        self._webhooks: List[Callable] = []

    def webhook(
        self,
        pipeline: str,
        integration: str,
        path: str,
        method: str = "POST",
        test_payload: Optional[Any] = None,
    ):
        """
        Metadata-only decorator for webhook handlers.
        Registers the handler in a shared registry for discovery.
        """

        def decorator(func: Callable):
            source_locator = None
            try:
                # Try to unwrap if it was already wrapped (e.g. by our own runtime, though typically it's raw here)
                target_func = inspect.unwrap(func)
                source_file = inspect.getsourcefile(target_func)
                source_lines, start_line = inspect.getsourcelines(target_func)
                source_locator = {
                    "file": source_file,
                    "line": start_line,
                    "name": target_func.__name__,
                    "qualname": getattr(
                        target_func, "__qualname__", target_func.__name__
                    ),
                    "module": target_func.__module__,
                }
            except Exception:
                pass

            # Attach metadata to func
            metadata = {
                "kind": "webhook",
                "pipeline": pipeline,
                "integration": integration,
                "path": path,
                "method": method,
                "test_payload": test_payload,
                "source_locator": source_locator,
            }
            setattr(func, "_ingress_metadata", metadata)
            self._webhooks.append(func)
            return func

        return decorator

    def get_webhooks(self) -> List[Callable]:
        """Return all registered webhook handlers."""
        return self._webhooks

    def poll(
        self,
        pipeline: str,
        integration: str,
        schedule: Union[str, "Schedule"],
        name: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
    ):
        """
        Scheduler entrypoint for polling.
        Behaves as a specialized integration_task that manages state.
        Injects 'state: dict' as the first argument.
        """
        from .decorators import TaskWrapper
        from .queue.backend import Schedule

        def decorator(func: Callable):
            ingress_name = name or func.__name__

            actual_schedule = (
                Schedule(cron=schedule) if isinstance(schedule, str) else schedule
            )

            @functools.wraps(func)
            async def state_wrapper(*args, **kwargs):
                with integration_context(
                    integration=integration,
                    integration_pipeline=pipeline,
                    ingress_name=ingress_name,
                ):
                    ctx = current_context()
                    if not ctx:
                        # Should not happen as we just created/joined one
                        raise RuntimeError(
                            "ingress.poll must run within an integration context"
                        )

                    # 1. Load state
                    state = State.get(ingress_name, scope="ingress") or {}

                    # 2. Inject as first arg and call
                    try:
                        if inspect.iscoroutinefunction(func):
                            result = await func(state, *args, **kwargs)
                        else:
                            result = func(state, *args, **kwargs)
                    except Exception:
                        # Do not save on exception
                        raise

                    # 3. Save state on success
                    State.set(ingress_name, state, scope="ingress")
                    return result

            tw = TaskWrapper(
                state_wrapper,
                {
                    "integration": integration,
                    "pipeline": pipeline,
                    "name": ingress_name,
                    "default_schedule": actual_schedule,
                    "tags": tags,
                },
            )

            # Attach metadata for discovery
            setattr(
                tw,
                "_ingress_metadata",
                {
                    "kind": "poll",
                    "pipeline": pipeline,
                    "integration": integration,
                    "schedule": actual_schedule,
                    "name": ingress_name,
                    "tags": tags,
                },
            )

            return tw

        return decorator


class FromFile:
    """Helper to specify a test payload loaded from a local JSON fixture."""

    def __init__(self, path: str):
        self.path = path


ingress = Ingress()

# Module-level aliases for the default Ingress instance
webhook = ingress.webhook
poll = ingress.poll
get_webhooks = ingress.get_webhooks

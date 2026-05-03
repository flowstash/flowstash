import functools
import inspect
import asyncio
import uuid
import secrets
from typing import Any, Callable, Optional, TypeVar, Awaitable, Union, Protocol, Mapping
from .context import integration_context, current_context, IntegrationContext
from opentelemetry import trace
from .queue.backend import Schedule, get_backend, register_task_schedule
from .observability.ingestion import _enqueue_lifecycle, record_run_scheduled


def get_tracer():
    """Return a tracer for the framework."""
    return trace.get_tracer("integrator-framework")


T = TypeVar("T")


class JobHandle(Protocol):
    id: str
    tags: Mapping[str, Any]

    def status(self) -> str: ...
    async def result(self, timeout: Optional[float] = None) -> Any: ...
    def cancel(self) -> bool: ...


def integration_step(
    *,
    integration: str,
    integration_pipeline: str,
    name: Optional[str] = None,
    tags: Optional[Mapping[str, Any]] = None,
):
    """
    Decorator for an immediate integration step.
    Always creates/joins context and records a run (root) or span (nested).
    name defaults to the decorated function's name.
    """

    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            effective_span_name = name or func.__name__
            tracer = get_tracer()
            otel_span_name = effective_span_name

            with integration_context(
                integration=integration,
                integration_pipeline=integration_pipeline,
                span_name=effective_span_name,
                tags=tags,
            ):
                ctx = current_context()

                with tracer.start_as_current_span(
                    otel_span_name,
                    attributes={
                        "fw.integration": ctx.integration,
                        "fw.pipeline": ctx.integration_pipeline,
                        "fw.run_id": ctx.run_id,
                        "code.function": func.__name__,
                        **{f"tag.{k}": v for k, v in ctx.tags.items()},
                    },
                ):
                    if inspect.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            effective_span_name = name or func.__name__
            otel_span_name = effective_span_name

            with integration_context(
                integration=integration,
                integration_pipeline=integration_pipeline,
                span_name=effective_span_name,
                tags=tags,
            ):
                ctx = current_context()

                with tracer.start_as_current_span(
                    otel_span_name,
                    attributes={
                        "fw.integration": ctx.integration,
                        "fw.pipeline": ctx.integration_pipeline,
                        "fw.run_id": ctx.run_id,
                        "code.function": func.__name__,
                        **{f"tag.{k}": v for k, v in ctx.tags.items()},
                    },
                ):
                    return func(*args, **kwargs)

        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper

    return decorator


class TaskWrapper:
    def __init__(self, func: Callable, metadata: dict):
        self.func = func
        self.metadata = metadata
        self._backend_handler = None  # Lazily populated by the backend if needed
        functools.update_wrapper(self, func)

        # Register task for lazy backend configuration
        from .queue.backend import register_task_wrapper

        register_task_wrapper(self)

        # If a default schedule is provided, try to register it with the backend.
        if "default_schedule" in self.metadata and self.metadata["default_schedule"]:
            register_task_schedule(
                self,  # Pass the wrapper itself so the backend can attach schedules to the actor
                self.metadata["default_schedule"],
                tags=self.metadata.get("tags"),
            )

    async def run(self, *args, **kwargs) -> Any:
        """Execute immediately in-process."""
        tracer = get_tracer()
        effective_span_name = self.metadata.get("span_name") or self.func.__name__
        otel_span_name = effective_span_name

        with integration_context(
            integration=self.metadata["integration"],
            integration_pipeline=self.metadata["pipeline"],
            span_name=effective_span_name,
            tags=self.metadata.get("tags"),
        ):
            ctx = current_context()

            with tracer.start_as_current_span(
                otel_span_name,
                attributes={
                    "fw.integration": ctx.integration,
                    "fw.pipeline": ctx.integration_pipeline,
                    "fw.run_id": ctx.run_id,
                    "code.function": self.func.__name__,
                    **{f"tag.{k}": v for k, v in ctx.tags.items()},
                },
            ):
                if inspect.iscoroutinefunction(self.func):
                    return await self.func(*args, **kwargs)
                else:
                    return self.func(*args, **kwargs)

    def submit(self, *args, **kwargs) -> JobHandle:
        """Enqueue to technical backend."""
        from .queue.backend import get_backend

        backend = get_backend()

        ctx = current_context()
        is_subtask = ctx is not None

        if not ctx:
            trace_id = secrets.token_hex(16)
            span_id = secrets.token_hex(8)
            ctx = IntegrationContext(
                integration=self.metadata["integration"],
                integration_pipeline=self.metadata["pipeline"],
                run_id=str(uuid.uuid4()),
                traceparent=f"00-{trace_id}-{span_id}-01",
                tags=self.metadata.get("tags") or {},
            )
        _enqueue_lifecycle(record_run_scheduled, correlation=ctx.corelation)

        return backend.submit(
            self._backend_handler or self.func,
            args,
            kwargs,
            context=ctx,
            integration=self.metadata["integration"],
            pipeline=self.metadata["pipeline"],
            tags={**(self.metadata.get("tags") or {}), "fw.is_subtask": is_subtask},
        )

    def schedule(
        self, eta_or_delay: Union[int, float, Any], *args, **kwargs
    ) -> JobHandle:
        """Schedule for future execution."""
        from .queue.backend import get_backend

        backend = get_backend()

        ctx = current_context()
        is_subtask = ctx is not None

        if not ctx:
            trace_id = secrets.token_hex(16)
            span_id = secrets.token_hex(8)
            ctx = IntegrationContext(
                integration=self.metadata["integration"],
                integration_pipeline=self.metadata["pipeline"],
                run_id=str(uuid.uuid4()),
                traceparent=f"00-{trace_id}-{span_id}-01",
                tags=self.metadata.get("tags") or {},
            )
        _enqueue_lifecycle(record_run_scheduled, correlation=ctx.corelation, attrs={"delay": eta_or_delay})

        return backend.schedule(
            self._backend_handler or self.func,
            args,
            kwargs,
            eta_or_delay=eta_or_delay,
            context=ctx,
            integration=self.metadata["integration"],
            pipeline=self.metadata["pipeline"],
            tags={**(self.metadata.get("tags") or {}), "fw.is_subtask": is_subtask},
        )

    def __call__(self, *args, **kwargs) -> JobHandle:
        return self.submit(*args, **kwargs)


def integration_task(
    *,
    integration: str,
    integration_pipeline: str | None = None,
    queue: Optional[str] = None,
    name: Optional[str] = None,
    tags: Optional[Mapping[str, Any]] = None,
    default_schedule: Optional[Schedule] = None,
    **kwargs,
):
    """
    Decorator for a task that can be executed inline or submitted to a queue.
    span_name defaults to the decorated function's name.
    """

    def decorator(func: Callable[..., Any]):
        return TaskWrapper(
            func,
            {
                "integration": integration,
                "pipeline": integration_pipeline,
                "queue": queue,
                "name": name or func.__name__,
                "tags": tags,
                "default_schedule": default_schedule,
            },
        )

    return decorator

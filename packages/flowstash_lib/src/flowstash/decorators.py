import functools
import inspect
import asyncio
import logging
import threading
import time
import uuid
import secrets
import warnings
import weakref
from datetime import datetime, UTC
from typing import Any, Callable, Optional, TypeVar, Awaitable, Union, Protocol, Mapping
from .context import integration_context, current_context, IntegrationContext
from opentelemetry import trace
from .queue.backend import Schedule, get_backend, register_task_schedule
from .observability.ingestion import (
    _enqueue_lifecycle,
    record_span_started,
    record_span_ended,
    normalize_arguments,
)


def get_tracer():
    """Return a tracer for the framework."""
    return trace.get_tracer("integrator-framework")


T = TypeVar("T")

logger = logging.getLogger(__name__)


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
            normalized_args = normalize_arguments(func, args, kwargs)

            with integration_context(
                integration=integration,
                integration_pipeline=integration_pipeline,
                span_name=effective_span_name,
                tags=tags,
                attrs={"args": normalized_args},
                metadata={"fw.span_kind": "step"},
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
                        **{f"arg.{k}": str(v) for k, v in normalized_args.items()},
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
            normalized_args = normalize_arguments(func, args, kwargs)

            with integration_context(
                integration=integration,
                integration_pipeline=integration_pipeline,
                span_name=effective_span_name,
                tags=tags,
                attrs={"args": normalized_args},
                metadata={"fw.span_kind": "step"},
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
                        **{f"arg.{k}": str(v) for k, v in normalized_args.items()},
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
        effective_span_name = (
            self.metadata.get("span_name")
            or self.metadata.get("name")
            or self.func.__name__
        )
        otel_span_name = effective_span_name
        normalized_args = normalize_arguments(self.func, args, kwargs)

        with integration_context(
            integration=self.metadata["integration"],
            integration_pipeline=self.metadata["pipeline"],
            span_name=effective_span_name,
            tags=self.metadata.get("tags"),
            attrs={"args": normalized_args},
            metadata={"fw.span_kind": "task"},
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
                    **{f"arg.{k}": str(v) for k, v in normalized_args.items()},
                },
            ):
                if inspect.iscoroutinefunction(self.func):
                    return await self.func(*args, **kwargs)
                else:
                    return self.func(*args, **kwargs)

    def _emit_delegation_span_and_call_backend(
        self,
        ctx: IntegrationContext,
        args: tuple,
        kwargs: dict,
        *,
        eta_or_delay: Optional[Any] = None,
        schedule_time: Optional[Any] = None,
    ) -> "JobHandle":
        """Record a DELEGATED span around the backend call and return the handle."""
        from .observability.model import TaskDelegationMetadata
        from datetime import datetime, UTC

        backend = get_backend()
        target_task = f"{self.func.__module__}.{self.func.__name__}"
        operation_id = str(uuid.uuid4())
        span_name = f"delegate:{target_task}"
        start_time = datetime.now(UTC)
        normalized_args = normalize_arguments(self.func, args, kwargs)

        delegation = TaskDelegationMetadata(
            parent_run_id=ctx.run_id,
            operation_id=operation_id,
            target_task=target_task,
            schedule_time=schedule_time,
        )

        span_metadata: dict = {
            "fw.span_kind": "delegation",
            "fw.operation_id": operation_id,
            "fw.target_task": target_task,
        }
        if schedule_time is not None:
            span_metadata["fw.schedule_time"] = schedule_time.isoformat()

        _enqueue_lifecycle(
            record_span_started,
            name=span_name,
            correlation=ctx.corelation,
            attrs={"args": normalized_args},
            metadata=span_metadata,
        )

        try:
            if eta_or_delay is not None:
                handle = backend.schedule(
                    self._backend_handler or self.func,
                    args,
                    kwargs,
                    eta_or_delay=eta_or_delay,
                    context=ctx,
                    integration=self.metadata["integration"],
                    pipeline=self.metadata["pipeline"],
                    tags=self.metadata.get("tags") or {},
                    delegation=delegation,
                )
            else:
                handle = backend.submit(
                    self._backend_handler or self.func,
                    args,
                    kwargs,
                    context=ctx,
                    integration=self.metadata["integration"],
                    pipeline=self.metadata["pipeline"],
                    tags=self.metadata.get("tags") or {},
                    delegation=delegation,
                )
        except Exception:
            _enqueue_lifecycle(
                record_span_ended,
                name=span_name,
                correlation=ctx.corelation,
                status="ERROR",
                start_time=start_time,
                end_time=datetime.now(UTC),
                metadata={**span_metadata, "fw.outcome": "ERROR"},
            )
            raise

        _enqueue_lifecycle(
            record_span_ended,
            name=span_name,
            correlation=ctx.corelation,
            status="DELEGATED",
            start_time=start_time,
            end_time=datetime.now(UTC),
            metadata={
                **span_metadata,
                "fw.outcome": "DELEGATED",
                "fw.accepted_id": handle.id if handle else None,
            },
        )
        return handle

    def submit(self, *args, **kwargs) -> JobHandle:
        """Enqueue to backend, recording a DELEGATED span in the current run."""
        ctx = current_context()
        if ctx is None:
            # No active run: open a short-lived root run scoped to this delegation call.
            normalized_args = normalize_arguments(self.func, args, kwargs)
            with integration_context(
                integration=self.metadata["integration"],
                integration_pipeline=self.metadata["pipeline"],
                tags=self.metadata.get("tags") or {},
                attrs={"args": normalized_args},
            ):
                return self._emit_delegation_span_and_call_backend(
                    current_context(), args, kwargs
                )
        return self._emit_delegation_span_and_call_backend(ctx, args, kwargs)

    def schedule(
        self, eta_or_delay: Union[int, float, datetime], *args: Any, **kwargs: Any
    ) -> JobHandle:
        """Schedule for future execution, recording a DELEGATED span in the current run.

        Args:
            eta_or_delay: Delay in seconds (``int`` or ``float``) or an explicit
                ``datetime`` for the target execution time. When a numeric value
                is given, it is treated as a delay in **seconds** from now.
            *args: Positional arguments forwarded to the task function.
            **kwargs: Keyword arguments forwarded to the task function.

        Returns:
            A :class:`JobHandle` for the scheduled job.
        """
        schedule_time = _compute_schedule_time(eta_or_delay)

        ctx = current_context()
        if ctx is None:
            normalized_args = normalize_arguments(self.func, args, kwargs)
            with integration_context(
                integration=self.metadata["integration"],
                integration_pipeline=self.metadata["pipeline"],
                tags=self.metadata.get("tags") or {},
                attrs={"args": normalized_args},
            ):
                return self._emit_delegation_span_and_call_backend(
                    current_context(),
                    args,
                    kwargs,
                    eta_or_delay=eta_or_delay,
                    schedule_time=schedule_time,
                )
        return self._emit_delegation_span_and_call_backend(
            ctx, args, kwargs, eta_or_delay=eta_or_delay, schedule_time=schedule_time
        )

    def __call__(self, *args, **kwargs) -> "TaskInvocation":
        """Bind arguments for delegation without dispatching yet.

        The returned :class:`TaskInvocation` satisfies the JobHandle protocol
        and submits on first use — for a bare fire-and-forget statement this
        happens at end of statement (via its finalizer), so plain
        ``my_task(x)`` still enqueues immediately. Chain ``.schedule(...)`` to
        dispatch as a scheduled job instead::

            my_task(x).schedule(eta_or_delay=25 * 60)
        """
        return TaskInvocation(self, args, kwargs)


def _compute_schedule_time(
    eta_or_delay: Union[int, float, datetime],
) -> Optional[datetime]:
    """Resolve eta_or_delay (seconds from now, or an explicit datetime) to UTC."""
    if isinstance(eta_or_delay, datetime):
        if eta_or_delay.tzinfo is None:
            return eta_or_delay.replace(tzinfo=UTC)
        return eta_or_delay.astimezone(UTC)
    if isinstance(eta_or_delay, (int, float)):
        return datetime.fromtimestamp(time.time() + eta_or_delay, tz=UTC)
    return None


_pending_invocations: "weakref.WeakSet[TaskInvocation]" = weakref.WeakSet()
_pending_lock = threading.Lock()


def _dispatch_invocation(
    task: TaskWrapper,
    args: tuple,
    kwargs: dict,
    ctx: Optional[IntegrationContext],
    state: dict,
    *,
    eta_or_delay: Optional[Union[int, float, datetime]] = None,
    schedule_time: Optional[datetime] = None,
    must_be_fresh: bool = False,
) -> JobHandle:
    """Dispatch a bound invocation exactly once, from whichever path gets there first."""
    with state["lock"]:
        if state["dispatched"]:
            if must_be_fresh:
                raise RuntimeError(
                    f"Task invocation of {task.metadata.get('name')!r} was already "
                    "dispatched; call .schedule() before using the returned handle."
                )
            return state["handle"]
        state["dispatched"] = True
    try:
        if ctx is not None:
            handle = task._emit_delegation_span_and_call_backend(
                ctx,
                args,
                kwargs,
                eta_or_delay=eta_or_delay,
                schedule_time=schedule_time,
            )
        else:
            # No run was active at call time: open a short-lived root run.
            normalized_args = normalize_arguments(task.func, args, kwargs)
            with integration_context(
                integration=task.metadata["integration"],
                integration_pipeline=task.metadata["pipeline"],
                tags=task.metadata.get("tags") or {},
                attrs={"args": normalized_args},
            ):
                handle = task._emit_delegation_span_and_call_backend(
                    current_context(),
                    args,
                    kwargs,
                    eta_or_delay=eta_or_delay,
                    schedule_time=schedule_time,
                )
    except BaseException:
        # Failed dispatches don't count: allow an explicit retry (or the
        # finalizer/flush) to attempt again.
        with state["lock"]:
            state["dispatched"] = False
        raise
    state["handle"] = handle
    return handle


def _finalize_dispatch(
    task: TaskWrapper,
    args: tuple,
    kwargs: dict,
    ctx: Optional[IntegrationContext],
    state: dict,
) -> None:
    """Finalizer for fire-and-forget invocations.

    On CPython a bare ``my_task(x)`` statement drops its reference at end of
    statement, landing here synchronously; otherwise this runs at GC or, at
    the latest, interpreter shutdown. Errors cannot propagate from a
    finalizer, so they are logged and warned instead.
    """
    if state["dispatched"]:
        return
    try:
        _dispatch_invocation(task, args, kwargs, ctx, state)
    except Exception:
        task_name = task.metadata.get("name", task.func.__name__)
        logger.exception("Fire-and-forget dispatch of task %r failed", task_name)
        try:
            warnings.warn(
                f"flowstash: fire-and-forget dispatch of task {task_name!r} failed; "
                "use .submit() for eager error propagation",
                RuntimeWarning,
                stacklevel=2,
            )
        except Exception:
            pass


def flush_pending_invocations(run_id: Optional[str] = None) -> int:
    """Dispatch every not-yet-dispatched :class:`TaskInvocation` as a submit.

    With ``run_id``, only invocations captured under that run are dispatched;
    a root ``integration_context`` calls this automatically on exit so every
    delegation created during a run is on the backend by the time the run
    ends. Returns the number of invocations dispatched. Errors propagate.
    """
    with _pending_lock:
        pending = list(_pending_invocations)
    dispatched = 0
    for inv in pending:
        if inv._state["dispatched"]:
            continue
        if run_id is not None and (inv._ctx is None or inv._ctx.run_id != run_id):
            continue
        inv.submit()
        dispatched += 1
    return dispatched


class TaskInvocation:
    """A bound-but-not-yet-dispatched task call, returned by ``TaskWrapper.__call__``.

    Dispatches exactly once:

    * as a scheduled job when ``.schedule(...)`` is chained,
    * as an immediate submit on explicit ``.submit()`` or first use of the
      JobHandle protocol (``.id``, ``.status()``, ``.result()``, ``.cancel()``),
    * otherwise automatically — at end of statement for bare fire-and-forget
      calls on CPython, when the enclosing run exits, or at interpreter
      shutdown at the latest.
    """

    def __init__(self, task: TaskWrapper, args: tuple, kwargs: dict):
        self._task = task
        self._args = args
        self._kwargs = kwargs
        # Capture now — dispatch may happen after the integration_context exited.
        self._ctx = current_context()
        # Shared with the finalizer, which must not reference self (a closure
        # over self would keep the invocation alive forever).
        self._state = {"dispatched": False, "handle": None, "lock": threading.Lock()}
        self._finalizer = weakref.finalize(
            self, _finalize_dispatch, task, args, kwargs, self._ctx, self._state
        )
        with _pending_lock:
            _pending_invocations.add(self)

    def submit(self) -> JobHandle:
        """Dispatch now as an immediate submit (idempotent)."""
        return _dispatch_invocation(
            self._task, self._args, self._kwargs, self._ctx, self._state
        )

    def schedule(self, eta_or_delay: Union[int, float, datetime]) -> JobHandle:
        """Dispatch as a scheduled job.

        Args:
            eta_or_delay: Delay in **seconds** from now (``int`` or ``float``),
                or an explicit ``datetime`` for the target execution time.

        Raises:
            RuntimeError: if this invocation was already dispatched.
        """
        return _dispatch_invocation(
            self._task,
            self._args,
            self._kwargs,
            self._ctx,
            self._state,
            eta_or_delay=eta_or_delay,
            schedule_time=_compute_schedule_time(eta_or_delay),
            must_be_fresh=True,
        )

    # --- JobHandle protocol: any use dispatches as a plain submit ---

    @property
    def id(self) -> str:
        return self.submit().id

    @property
    def tags(self) -> Mapping[str, Any]:
        return self.submit().tags

    def status(self) -> str:
        return self.submit().status()

    async def result(self, timeout: Optional[float] = None) -> Any:
        return await self.submit().result(timeout)

    def cancel(self) -> bool:
        return self.submit().cancel()

    def __repr__(self) -> str:
        # Deliberately side-effect free: debuggers evaluate repr on hover and
        # must not trigger a dispatch.
        state = "dispatched" if self._state["dispatched"] else "pending"
        return f"<TaskInvocation {self._task.metadata.get('name')!r} {state}>"


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

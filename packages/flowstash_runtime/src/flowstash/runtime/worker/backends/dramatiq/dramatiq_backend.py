"""
Dramatiq backend implementation for framework task queue.

Moved from flowstash to keep flowstash reusable without hard dependency on Dramatiq.
"""

import dramatiq
import logging
import asyncio
import threading
from typing import Any, Callable, Optional, Mapping, List, Union
from flowstash.queue.backend import JobHandle, TaskBackend, Schedule
from flowstash.context import IntegrationContext, integration_context, current_context
from opentelemetry import trace, baggage
from flowstash.observability.ingestion import (
    record_run_started,
    record_run_ended,
    record_span_started,
    record_span_ended,
)


class AsyncRunner(threading.local):
    """
    Provides a per-thread asyncio event loop strategy for Dramatiq worker threads.

    Dramatiq spawns many OS threads.  Each thread needs its *own* event loop so
    that async Redis / HTTP clients are never shared across loops – sharing is the
    root cause of the ``RuntimeError: Task got Future attached to a different loop``
    error.

    ``threading.local`` guarantees that ``__init__`` is called once per thread,
    so we always create a brand-new loop, set it as the thread's current loop,
    and close it when the thread exits.
    """

    def __init__(self):
        super().__init__()
        # Always create a fresh, dedicated loop for this thread.
        # Never reuse get_event_loop() – it may return the main thread's loop.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self.run, self.fire = self._build_strategy(loop)

    def _build_strategy(self, loop):
        def execute_run(coro):
            # loop is never running here – we're the only driver in this thread.
            return loop.run_until_complete(coro)

        def execute_fire(coro):
            try:
                loop.run_until_complete(coro)
            except Exception:
                pass

        return execute_run, execute_fire


_async_runner = AsyncRunner()


def get_tracer():
    """Return a tracer for the framework."""
    return trace.get_tracer("integrator-framework")


class DramatiqJobHandle:
    def __init__(
        self,
        message: dramatiq.Message,
        tags: Optional[Mapping[str, Any]] = None,
        scheduled_job_id: Optional[str] = None,
    ):
        self.message = message
        self.id = message.message_id
        self.tags = tags or {}
        self.scheduled_job_id = scheduled_job_id
        self.schedule = None

    def status(self) -> str:
        # Dramatiq doesn't provide status out of the box without a results backend
        return "unknown"

    async def result(self, timeout: Optional[float] = None) -> Any:
        # Standard Dramatiq doesn't support awaiting results without a backend
        raise NotImplementedError("Result backend not configured for Dramatiq.")

    def cancel(self) -> bool:
        # Dramatiq doesn't support easy cancellation once enqueued
        return False


class DramatiqBackend(TaskBackend):
    def __init__(self):
        # Internal registry to track scheduled jobs
        self._scheduled_jobs: List[dict] = []

    def configure_task(self, wrapper: Any) -> None:
        """
        Configure the task wrapper as a Dramatiq actor if not already done.
        """
        import inspect

        if getattr(wrapper, "_backend_handler", None) is not None:
            return  # already configured

        original_func = wrapper.func
        name = wrapper.metadata.get("name") or getattr(
            original_func, "__name__", str(original_func)
        )
        queue = wrapper.metadata.get("queue") or "default"

        if inspect.iscoroutinefunction(original_func):
            # Convert to a sync function that runs via asyncio
            def sync_actor(*args, **kwargs):
                return _async_runner.run(original_func(*args, **kwargs))

            sync_actor.__name__ = original_func.__name__
            sync_actor.__module__ = original_func.__module__
            sync_actor.__qualname__ = original_func.__qualname__
            actor = dramatiq.actor(sync_actor, actor_name=name, queue_name=queue)
        else:
            actor = dramatiq.actor(original_func, actor_name=name, queue_name=queue)

        # Store for future submit calls
        wrapper._backend_handler = actor

    def _prepare_headers(
        self,
        context: Optional[IntegrationContext],
        tags: Optional[Mapping[str, Any]],
        scheduled_job_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        headers = {}
        if context:
            headers["fw.integration"] = context.integration
            headers["fw.pipeline"] = context.integration_pipeline
            headers["fw.run_id"] = context.run_id
            if context.traceparent:
                headers["traceparent"] = context.traceparent
            # Inherit and merge tags from context
            merged_tags = {**context.tags, **(tags or {})}
        else:
            merged_tags = tags or {}

        if merged_tags:
            headers["fw.tags"] = merged_tags

        # Add scheduled_job_id as DEDICATED header (not in tags)
        if scheduled_job_id:
            headers["fw.scheduled_job_id"] = scheduled_job_id

        return headers

    def submit(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        context: Optional[IntegrationContext] = None,
        integration: Optional[str] = None,
        pipeline: Optional[str] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> JobHandle:
        if not hasattr(func, "send"):
            raise ValueError(f"Function {func.__name__} is not a Dramatiq actor.")

        headers = self._prepare_headers(context, tags)
        # Prioritize explicit integration/pipeline
        if integration:
            headers["fw.integration"] = integration
        if pipeline:
            headers["fw.pipeline"] = pipeline

        # Ensure we have the subtask flag if not already there
        if "fw.is_subtask" not in (tags or {}):
            headers.setdefault("fw.tags", {})["fw.is_subtask"] = context is not None

        message = func.send_with_options(args=args, kwargs=kwargs, headers=headers)
        return DramatiqJobHandle(message, tags=headers.get("fw.tags"))

    def schedule(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        eta_or_delay: Union[int, float, Any],
        context: Optional[IntegrationContext] = None,
        integration: Optional[str] = None,
        pipeline: Optional[str] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> JobHandle:
        if not hasattr(func, "send_with_options"):
            raise ValueError(f"Function {func.__name__} is not a Dramatiq actor.")

        headers = self._prepare_headers(context, tags)
        # Prioritize explicit integration/pipeline
        if integration:
            headers["fw.integration"] = integration
        if pipeline:
            headers["fw.pipeline"] = pipeline

        delay = eta_or_delay if isinstance(eta_or_delay, (int, float)) else None
        # Handle datetime eta if needed...

        message = func.send_with_options(
            args=args, kwargs=kwargs, delay=delay, headers=headers
        )
        return DramatiqJobHandle(message, tags=headers.get("fw.tags"))

    def register_schedule(
        self,
        func: Callable,
        schedule: Schedule,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Register a schedule for a task using APScheduler.

        This stores the registration information for later retrieval by the consumer.
        """
        # Extract actor if wrapped
        if hasattr(func, "_backend_handler") and func._backend_handler is not None:
            actor_func = func._backend_handler
        else:
            actor_func = func

        # Auto-generate scheduled_job_id from function path
        # Dramatiq actors wrap the function, need to access the underlying fn
        if hasattr(actor_func, "fn"):
            underlying_fn = actor_func.fn
            scheduled_job_id = f"{underlying_fn.__module__}.{underlying_fn.__name__}"
        else:
            scheduled_job_id = f"{getattr(actor_func, '__module__', 'unknown')}.{getattr(actor_func, '__name__', 'unknown')}"

        logger = logging.getLogger(__name__)
        func_name = getattr(
            actor_func, "actor_name", getattr(actor_func, "__name__", str(actor_func))
        )
        logger.info(
            f"Registering schedule for {func_name} with ID {scheduled_job_id}: {schedule.cron} "
            f"(args={args}, kwargs={kwargs}, tags={tags})"
        )

        # Track this scheduled job internally
        self._scheduled_jobs.append(
            {
                "func": func,
                "schedule": schedule,
                "scheduled_job_id": scheduled_job_id,  # Store as dedicated field
                "args": args or (),
                "kwargs": kwargs or {},
                "tags": tags or {},
                "actor_name": func_name,  # Use the already computed name
            }
        )

    def get_scheduled_jobs(self) -> List[JobHandle]:
        """
        Return all scheduled jobs from the internal registry.

        We return our tracked registrations as pseudo-JobHandles for inspection.
        """
        handles = []
        for job_info in self._scheduled_jobs:
            # Create a pseudo-handle for scheduled jobs
            # Note: Scheduled jobs don't have message IDs until they execute
            class ScheduledJobHandle:
                def __init__(self, job_info):
                    self.id = f"scheduled:{job_info['scheduled_job_id']}"
                    self.tags = job_info["tags"]
                    self.scheduled_job_id = job_info[
                        "scheduled_job_id"
                    ]  # Dedicated field
                    self.schedule = job_info["schedule"].cron  # CRON expression
                    self._job_info = job_info

                def status(self) -> str:
                    return "scheduled"

                async def result(self, timeout: Optional[float] = None) -> Any:
                    raise NotImplementedError(
                        "Scheduled jobs don't have results until executed."
                    )

                def cancel(self) -> bool:
                    # Not implemented
                    return False

            handles.append(ScheduledJobHandle(job_info))

        return handles


class FrameworkContextMiddleware(dramatiq.Middleware):
    """
    Dramatiq middleware to propagate IntegrationContext and OTel trace context via message headers.
    """

    def __init__(self):
        super().__init__()
        self.local = threading.local()

    def before_process_message(self, broker, message):
        headers = message.options.get("headers", {})

        integration = headers.get("fw.integration")
        pipeline = headers.get("fw.pipeline")
        run_id = headers.get("fw.run_id")
        traceparent = headers.get("traceparent")
        tags = headers.get("fw.tags", {})
        scheduled_job_id = headers.get("fw.scheduled_job_id")  # Extract dedicated field

        # Create/join integration context
        ctx_mgr = integration_context(
            integration=integration,
            integration_pipeline=pipeline,
            run_id=run_id,
            tags=tags,
        )
        ctx = ctx_mgr.__enter__()

        self.local.ctx_mgr = ctx_mgr
        self.local.ctx = ctx

        # Determine if this is a subtask
        self.local.is_subtask = tags.get("fw.is_subtask", False)
        self.local.scheduled_job_id = scheduled_job_id  # Store for use in run events
        self.local.span_name = f"task.execute:{message.actor_name}"

        # Sync observability recording (middleware is sync)
        def _fire(coro):
            _async_runner.fire(coro)

        if not getattr(self.local, "is_subtask", False):
            _fire(
                record_run_started(
                    correlation=self.local.ctx.corelation,
                    scheduled_job_id=getattr(self.local, "scheduled_job_id", None),
                )
            )
        else:
            _fire(
                record_run_started(
                    correlation=self.local.ctx.corelation, status="RUNNING_SUBTASK"
                )
            )  # Or Span?
            # Actually, user says "treat as span".
            _fire(
                record_span_started(
                    name=getattr(self.local, "span_name", "task"),
                    correlation=self.local.ctx.corelation,
                )
            )

        # Start a span for the execution
        tracer = get_tracer()
        span_mgr = tracer.start_as_current_span(
            getattr(self.local, "span_name", "task"),
            attributes={
                "fw.integration": self.local.ctx.integration,
                "fw.pipeline": self.local.ctx.integration_pipeline,
                "fw.run_id": self.local.ctx.run_id,
                **{f"tag.{k}": v for k, v in self.local.ctx.tags.items()},
            },
        )
        span = span_mgr.__enter__()
        self.local.span_mgr = span_mgr
        self.local.span = span

    def after_process_message(self, broker, message, *, result=None, exception=None):
        def _fire(coro):
            _async_runner.fire(coro)

        if hasattr(self, "local"):
            if hasattr(self.local, "span_mgr"):
                if exception and hasattr(self.local, "span"):
                    self.local.span.record_exception(exception)
                    self.local.span.set_status(trace.Status(trace.StatusCode.ERROR))
                self.local.span_mgr.__exit__(None, None, None)

            if hasattr(self.local, "ctx"):
                status = "SUCCEEDED" if not exception else "FAILED"
                if not getattr(self.local, "is_subtask", False):
                    _fire(
                        record_run_ended(
                            status=status,
                            correlation=self.local.ctx.corelation,
                            scheduled_job_id=getattr(
                                self.local, "scheduled_job_id", None
                            ),
                        )
                    )
                else:
                    _fire(
                        record_span_ended(
                            name=getattr(self.local, "span_name", "task"),
                            status="OK" if not exception else "ERROR",
                            correlation=self.local.ctx.corelation,
                            error_summary=str(exception) if exception else None,
                        )
                    )

            if hasattr(self.local, "ctx_mgr"):
                self.local.ctx_mgr.__exit__(None, None, None)

    def before_enqueue(self, broker, message, delay):
        # Tags are usually already injected by the backend in our design,
        # but let's ensure we propagate current context tags if not overridden.
        ctx = current_context()
        if ctx:
            headers = message.options.setdefault("headers", {})
            headers.setdefault("fw.integration", ctx.integration)
            headers.setdefault("fw.pipeline", ctx.integration_pipeline)
            headers.setdefault("fw.run_id", ctx.run_id)
            headers.setdefault("fw.tags", ctx.tags)

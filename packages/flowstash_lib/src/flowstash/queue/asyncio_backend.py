import asyncio
import uuid
from typing import Any, Callable, Optional, Mapping, List, Union
from .backend import TaskBackend, JobHandle, Schedule
from ..context import IntegrationContext, current_context, integration_context
from ..observability.ingestion import normalize_arguments
import logging


class AsyncioJobHandle:
    def __init__(self, task: asyncio.Task, tags: Mapping[str, Any]):
        self.id = str(uuid.uuid4())
        self._task = task
        self.tags = tags

    def status(self) -> str:
        if self._task.done():
            if self._task.cancelled():
                return "cancelled"
            if self._task.exception():
                return "failed"
            return "finished"
        return "running"

    async def result(self, timeout: Optional[float] = None) -> Any:
        if timeout:
            return await asyncio.wait_for(self._task, timeout)
        return await self._task

    def cancel(self) -> bool:
        return self._task.cancel()


class AsyncioBackend:
    """
    A TaskBackend that executes tasks immediately using asyncio.create_task.
    Useful for testing and local development without a worker process.

    Lifecycle is handled automatically by integration_context:
    - asyncio.create_task() copies the caller's contextvars snapshot.
    - If a parent run is active, the task inherits it -> auto-recorded as a span.
    - If no parent, the task starts a new run.
    """

    def __init__(self):
        self._scheduled_jobs: List[dict] = []
        self._submitted_tasks: List[asyncio.Task] = []

    def submit(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        context: Optional[IntegrationContext] = None,
        integration: Optional[str] = None,
        pipeline: Optional[str] = None,
        tags: Optional[Mapping[str, Any]] = None,
        delegation: Optional[Any] = None,
    ) -> JobHandle:
        # context is captured at submit() time (from the calling frame's contextvars).
        # Each submitted task is a new independent run — it must NOT inherit the parent
        # run_id. We break the contextvar chain explicitly inside the spawned task by
        # resetting _current_ctx to None before entering its own integration_context.
        ctx = context or current_context()

        parent_run_id = (
            delegation.parent_run_id if delegation else (ctx.run_id if ctx else None)
        )
        operation_id = delegation.operation_id if delegation else None

        async def _run_with_new_run():
            from ..context import _current_ctx as _ctx_var

            # Break inherited contextvar so integration_context treats this as a root run.
            token = _ctx_var.set(None)
            try:
                with integration_context(
                    span_name=func.__name__,
                    integration=integration or (ctx.integration if ctx else None),
                    integration_pipeline=pipeline
                    or (ctx.integration_pipeline if ctx else None),
                    # No run_id: a fresh one is allocated by integration_context
                    tags={**(ctx.tags if ctx else {}), **(tags or {})},
                    parent_run_id=parent_run_id,
                    operation_id=operation_id,
                    attrs={"args": normalize_arguments(func, args, kwargs)},
                ):
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
            finally:
                _ctx_var.reset(token)

        task = asyncio.create_task(_run_with_new_run())
        self._submitted_tasks.append(task)
        return AsyncioJobHandle(task, tags or {})

    async def drain(self):
        """Transitively drain all submitted tasks, including tasks spawned during execution."""
        while True:
            pending = [t for t in self._submitted_tasks if not t.done()]
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
        self._submitted_tasks.clear()

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
        raise NotImplementedError(
            "AsyncioBackend does not support scheduling. Use DramatiqBackend for scheduling."
        )

    def register_schedule(
        self,
        func: Callable,
        schedule: Schedule,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> None:
        # Resolve the underlying function name for display / ID purposes.
        # func is a TaskWrapper; its .func attribute is the raw Python function.
        raw_fn = getattr(func, "func", func)
        fn_name = getattr(raw_fn, "__name__", str(raw_fn))
        fn_module = getattr(raw_fn, "__module__", "unknown")
        scheduled_job_id = f"{fn_module}.{fn_name}"

        logging.getLogger(__name__).info(
            f"Registering async schedule for {fn_name} ({schedule.cron})"
        )
        self._scheduled_jobs.append(
            {
                "wrapper": func,
                "schedule": schedule,
                "scheduled_job_id": scheduled_job_id,
                "args": args or (),
                "kwargs": kwargs or {},
                "tags": tags or {},
                "func_name": fn_name,
            }
        )

    def get_scheduled_jobs(self) -> List[dict]:
        return list(self._scheduled_jobs)


class AsyncioFeedBackend:
    """
    A FeedBackend that executes feed consumers immediately using asyncio.create_task.
    Useful for testing and local development.
    """

    def __init__(self):
        # Tracks in-flight tasks keyed by "{subscription_name}:{dedupe_key}" to
        # prevent double-processing the same record (mirrors Redis deduplication).
        self._inflight: dict = {}

    async def publish(self, feed_id: str, record: Any) -> str:
        from ..pipelines.consumer import get_registered_consumers
        from ..context import get_context
        import inspect

        ctx = get_context()
        integration = ctx.integration if ctx else "unknown"
        dedupe_key = record.get_dedupe_key(integration)

        consumers = get_registered_consumers()
        for spec in consumers:
            if spec.feed_id == feed_id:
                inflight_key = f"{spec.subscription_name}:{dedupe_key}"
                existing = self._inflight.get(inflight_key)
                if existing is not None and not existing.done():
                    # A task for this dedupe key is already running — skip.
                    continue

                payload = [record] if spec.batch else record

                async def _run_consumer(handler, payload, name, _key=inflight_key):
                    try:
                        if inspect.iscoroutinefunction(handler):
                            await handler(payload)
                        else:
                            handler(payload)
                    except Exception as e:
                        logging.error(
                            f"Error in asyncio feed consumer {name}: {e}", exc_info=True
                        )
                    finally:
                        self._inflight.pop(_key, None)

                task = asyncio.create_task(
                    _run_consumer(spec.handler, payload, spec.subscription_name)
                )
                self._inflight[inflight_key] = task
                await task

        return dedupe_key

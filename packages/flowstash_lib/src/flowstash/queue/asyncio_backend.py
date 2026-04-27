import asyncio
import uuid
from typing import Any, Callable, Optional, Mapping, List, Union
from .backend import TaskBackend, JobHandle, Schedule
from ..context import IntegrationContext, current_context, integration_context
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
    ) -> JobHandle:
        # context is captured at submit() time (from the calling frame's contextvars).
        # asyncio.create_task() also copies the current contextvars snapshot, so the
        # spawned task already inherits the parent's _current_ctx — no explicit run_id
        # pass-through needed.
        ctx = context or current_context()

        async def _run_with_context():
            # integration_context auto-detects root vs nested via current_context():
            # - parent present (inherited via create_task contextvars copy) → span
            # - no parent → new run
            with integration_context(
                integration=integration or (ctx.integration if ctx else None),
                integration_pipeline=pipeline
                or (ctx.integration_pipeline if ctx else None),
                run_id=ctx.run_id if ctx else None,
                span_name=func.__name__,
                tags={**(ctx.tags if ctx else {}), **(tags or {})},
            ):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

        task = asyncio.create_task(_run_with_context())
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

    async def publish(self, feed_id: str, record: Any) -> str:
        from ..pipelines.consumer import get_registered_consumers
        from ..context import get_context
        import inspect

        consumers = get_registered_consumers()
        for spec in consumers:
            if spec.feed_id == feed_id:
                # Prepare payload
                payload = [record] if spec.batch else record

                async def _run_consumer(handler, payload, name):
                    try:
                        if inspect.iscoroutinefunction(handler):
                            await handler(payload)
                        else:
                            handler(payload)
                    except Exception as e:
                        logging.error(
                            f"Error in asyncio feed consumer {name}: {e}", exc_info=True
                        )

                asyncio.create_task(
                    _run_consumer(spec.handler, payload, spec.subscription_name)
                )

        ctx = get_context()
        integration = ctx.integration if ctx else "unknown"
        return record.get_dedupe_key(integration)

from __future__ import annotations
from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
    runtime_checkable,
    Mapping,
    List,
    Union,
    TYPE_CHECKING,
)
from pydantic import BaseModel
from ..context import IntegrationContext

if TYPE_CHECKING:
    from ..observability.model import TaskDelegationMetadata


@runtime_checkable
class JobHandle(Protocol):
    id: str
    tags: Mapping[str, Any]
    schedule: Optional[str] = None
    scheduled_job_id: Optional[str] = None

    def status(self) -> str: ...
    async def result(self, timeout: Optional[float] = None) -> Any: ...
    def cancel(self) -> bool: ...


class Schedule(BaseModel):
    cron: str


class TaskBackend(Protocol):
    def submit(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        context: Optional[IntegrationContext] = None,
        integration: Optional[str] = None,
        pipeline: Optional[str] = None,
        tags: Optional[Mapping[str, Any]] = None,
        delegation: Optional[TaskDelegationMetadata] = None,
    ) -> JobHandle: ...

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
        delegation: Optional[TaskDelegationMetadata] = None,
    ) -> JobHandle: ...

    def register_schedule(
        self,
        func: Callable,
        schedule: Schedule,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> None: ...

    def get_scheduled_jobs(self) -> List[JobHandle]: ...

    def on_worker_init(self) -> None:
        """Optional hook executed once during worker runtime initialization."""
        pass


_backend: Optional[TaskBackend] = None
_pending_schedules: List[tuple] = []
_registered_task_wrappers: List[Any] = []


def set_backend(backend: TaskBackend):
    global _backend
    _backend = backend

    # Process any schedules registered before the backend was set
    while _pending_schedules:
        func, schedule, args, kwargs, tags = _pending_schedules.pop(0)
        backend.register_schedule(func, schedule, args, kwargs, tags)

    # Configure any already-registered task wrappers
    if hasattr(backend, "configure_task"):
        for wrapper in _registered_task_wrappers:
            backend.configure_task(wrapper)

    return backend


def register_task_wrapper(wrapper: Any) -> None:
    """Register a task wrapper so the backend can lazily configure it."""
    _registered_task_wrappers.append(wrapper)
    if _backend and hasattr(_backend, "configure_task"):
        _backend.configure_task(wrapper)


def get_backend() -> TaskBackend:
    if _backend is None:
        # Fallback or error
        raise RuntimeError("No task backend configured. Call set_backend() first.")
    return _backend


def register_task_schedule(
    func: Callable,
    schedule: Schedule,
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    tags: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Register a schedule for a task. If backend is not yet set, it will be queued.
    """
    if _backend:
        _backend.register_schedule(func, schedule, args, kwargs, tags)
    else:
        _pending_schedules.append((func, schedule, args, kwargs, tags))

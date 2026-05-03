"""
Managed Tasks Backend — HTTP proxy to the Integrator Platform API.

This is a lightweight TaskBackend implementation that submits tasks and
registers schedules via the managed platform API. No GCP SDK dependency.
"""

import os
import json
import logging
import uuid
from dataclasses import asdict
from typing import Any, Callable, Optional, Mapping, List, Union, Dict

import httpx

from flowstash.queue.backend import TaskBackend, JobHandle, Schedule
from flowstash.context import IntegrationContext

logger = logging.getLogger(__name__)


class ManagedJobHandle:
    """Job handle for tasks submitted to the managed platform."""

    def __init__(self, task_id: str, tags: Optional[Mapping[str, Any]] = None):
        self.id = task_id
        self.tags = tags or {}
        self.schedule = None
        self.scheduled_job_id = None

    def status(self) -> str:
        return "submitted"

    async def result(self, timeout: Optional[float] = None) -> Any:
        raise NotImplementedError(
            "ManagedTasksBackend does not support awaiting results. "
            "Tasks are executed asynchronously via Cloud Tasks."
        )

    def cancel(self) -> bool:
        return False


class ManagedTasksBackend(TaskBackend):
    """
    TaskBackend that proxies requests to the Integrator Platform API.

    Requires:
    - api_url: Base URL of the platform API (e.g. https://api.flowstash.dev)
    - auth_token: JWT token for authentication
    - service_url: This worker's own URL (for Cloud Tasks callbacks).
      Defaults to SERVICE_URL env var. If neither is set the literal placeholder
      "SERVICE_URL" is sent and the managed API resolves it via Firestore.
    - project_id: Logical project identifier (injected as MANAGED_PROJECT_ID at deploy time).
    - environment: Deployment environment, e.g. "prod" or "dev"
      (injected as ENVIRONMENT at deploy time).
    """

    def __init__(
        self,
        api_url: str,
        auth_token: str,
        service_url: Optional[str] = None,
        project_id: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.auth_token = auth_token

        self.service_url = service_url or os.environ.get("SERVICE_URL") or "SERVICE_URL"
        self.project_id = (
            project_id
            or os.environ.get("MANAGED_PROJECT_ID")
            or os.environ.get("FLOWSTASH_PROJECT_ID")
        )
        self.environment = environment or os.environ.get("ENVIRONMENT")

        # Validate required configuration
        if not self.project_id:
            raise ValueError(
                "project_id is not set. Provide it as an argument or set MANAGED_PROJECT_ID env var."
            )

        if not self.environment:
            raise ValueError(
                "environment is not set. Provide it as an argument or set ENVIRONMENT env var."
            )

        self._client = httpx.Client(
            base_url=self.api_url,
            headers={
                "Authorization": f"Bearer {self.auth_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._registered_tasks: List[Dict[str, Any]] = []

    def _serialize_args(self, func: Callable, args: tuple, kwargs: dict) -> dict:
        """Serialize function reference and arguments to a JSON-safe payload."""
        func_ref = (
            f"{func.__module__}.{func.__name__}"
            if hasattr(func, "__module__")
            else str(func)
        )
        return {
            "func_ref": func_ref,
            "args": list(args),
            "kwargs": kwargs,
        }

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
        """Submit a task via the Platform API → Cloud Tasks."""
        target_url = f"{self.service_url}/handle_task"

        payload: Dict[str, Any] = {
            "target_url": target_url,
            "task_name": (
                f"{func.__module__}.{func.__name__}"
                if hasattr(func, "__module__")
                else str(func)
            ),
            "project_id": self.project_id,
            "environment": self.environment,
            "payload": {
                **self._serialize_args(func, args, kwargs),
                "integration": integration
                or (context.integration if context else None),
                "pipeline": pipeline
                or (context.integration_pipeline if context else None),
                # Do NOT pass trigger run_id as execution run_id.
                # The execution side allocates a fresh run_id.
                "tags": dict(tags or {}),
                "delegation": asdict(delegation) if delegation else None,
            },
        }

        response = self._client.post("/v1/tasks/submit", json=payload)
        response.raise_for_status()
        data = response.json()

        return ManagedJobHandle(
            task_id=data.get("task_id", str(uuid.uuid4())),
            tags=tags,
        )

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
        delegation: Optional[Any] = None,
    ) -> JobHandle:
        """Schedule a task for future execution via Cloud Tasks."""
        import time

        target_url = f"{self.service_url}/handle_task"

        # Convert delay (ms) to absolute schedule_time
        if isinstance(eta_or_delay, (int, float)):
            schedule_time = time.time() + (eta_or_delay / 1000.0)
        else:
            schedule_time = None

        payload: Dict[str, Any] = {
            "target_url": target_url,
            "task_name": (
                f"{func.__module__}.{func.__name__}"
                if hasattr(func, "__module__")
                else str(func)
            ),
            "project_id": self.project_id,
            "environment": self.environment,
            "payload": {
                **self._serialize_args(func, args, kwargs),
                "integration": integration
                or (context.integration if context else None),
                "pipeline": pipeline
                or (context.integration_pipeline if context else None),
                # Do NOT pass trigger run_id as execution run_id.
                "tags": dict(tags or {}),
                "delegation": asdict(delegation) if delegation else None,
            },
            "schedule_time": schedule_time,
        }

        response = self._client.post("/v1/tasks/submit", json=payload)
        response.raise_for_status()
        data = response.json()

        return ManagedJobHandle(
            task_id=data.get("task_id", str(uuid.uuid4())),
            tags=tags,
        )

    def register_schedule(
        self,
        func: Callable,
        schedule: Schedule,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        tags: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Register a scheduled task with the Platform API.

        The platform will create/update a Cloud Scheduler job
        for this cron expression and store the task definition in Firestore.
        """
        # Derive task_id from function path
        if hasattr(func, "func"):
            # TaskWrapper
            underlying = func.func
            task_id = f"{underlying.__module__}.{underlying.__name__}"
        elif hasattr(func, "fn"):
            # Dramatiq actor
            task_id = f"{func.fn.__module__}.{func.fn.__name__}"
        else:
            task_id = f"{func.__module__}.{func.__name__}"

        task_name = getattr(func, "__name__", str(func))

        # Read integration/pipeline from TaskWrapper metadata when available.
        # These are first-class fields on the wrapper, not tags.
        meta = getattr(func, "metadata", {})
        integration = meta.get("integration") or "unknown"
        pipeline = meta.get("pipeline") or "unknown"

        task_dict = {
            "task_id": task_id,
            "task_name": task_name,
            "target_url": f"{self.service_url}/handle_task",
            "integration": integration,
            "pipeline": pipeline,
            "default_schedule": schedule.cron,
        }
        self._registered_tasks.append(task_dict)
        logger.info(f"Registered schedule locally for {task_id}: {schedule.cron}")

    def get_scheduled_jobs(self) -> List[JobHandle]:
        """
        Scheduled jobs are managed server-side.
        Returns an empty list — scheduling state lives in Firestore.
        """
        return []

    def on_worker_init(self) -> None:
        """
        Hook executed once during initialization.
        With the pull model, schedules are fetched via worker HTTP endpoint.
        We do nothing here.
        """
        pass

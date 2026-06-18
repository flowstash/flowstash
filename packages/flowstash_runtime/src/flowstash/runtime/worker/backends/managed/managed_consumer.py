"""
Managed Consumer — Cloud Run Job task runner.

Reads sys.argv for:
    run-task <task_name> [pos_arg1 pos_arg2 ...] [--key1 val1 --key2 val2 ...]

Resolves the task, executes it inside an integration_context with full
observability, flushes, and exits 0 (success) or 1 (failure).
"""

import asyncio
import json
import logging
import os
import sys
import traceback
from typing import Any

from collections import defaultdict

from flowstash.pipelines.consumer import get_registered_consumers
import httpx

from flowstash.config.runtime_config import RuntimeConfig
from flowstash.context import integration_context
from flowstash.observability.ingestion import (
    AsyncManager,
    normalize_arguments,
    record_run_ended,
    record_run_started,
)
from flowstash.queue.consumer import TaskConsumer

from .task_resolver import _invoke_task_callable, resolve_function

logger = logging.getLogger(__name__)


MANAGED_JOB_COMMANDS = {
    "run-task",
    "register_schedules",
}


def is_managed_job_mode(argv: list[str] | None = None) -> bool:
    """Return True when the managed worker should run as a one-shot job."""
    tokens = sys.argv[1:] if argv is None else argv
    return bool(tokens) and tokens[0] in MANAGED_JOB_COMMANDS


# ─── Argument Parsing ───────────────────────────────────────────────


def _cast_value(s: str) -> Any:
    """Smart type-cast a CLI string value."""
    # Try int first (no decimal point, no exponent notation)
    try:
        return int(s)
    except ValueError:
        pass

    # Try float
    try:
        return float(s)
    except ValueError:
        pass

    # Detect JSON-like strings; fail loudly on malformed JSON
    if s.startswith(("{", "[", "true", "false", "null")):
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            print(f"[run-task] Invalid JSON argument {s!r}: {e}", file=sys.stderr)
            sys.exit(1)

    return s


def _parse_cli_args(raw: list) -> tuple:
    """
    Parse tokens following 'run-task' into (task_name, args, kwargs).

    Syntax:
        <task_name> [pos_arg ...] [--key value ...] [--flag]
    """
    if not raw:
        print(
            "Usage: worker_main.py run-task <task_name> [args...] [--key value ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    task_name = raw[0]
    tokens = raw[1:]

    args: list = []
    kwargs: dict = {}

    i = 0
    seen_flag = False
    while i < len(tokens):
        tok = tokens[i]
        if tok.startswith("-"):
            seen_flag = True
            key = tok.lstrip("-")
            # Peek at next token for the value
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
                kwargs[key] = _cast_value(tokens[i + 1])
                i += 2
            else:
                kwargs[key] = True
                i += 1
        else:
            if seen_flag:
                # Positional args after a flag are ambiguous — treat as error
                print(
                    f"[run-task] Positional argument {tok!r} found after a flag. "
                    "All positional args must come before flags.",
                    file=sys.stderr,
                )
                sys.exit(1)
            args.append(_cast_value(tok))
            i += 1

    return task_name, args, kwargs


# ─── Observability Helpers ───────────────────────────────────────────


async def _record_missing_task(task_name: str) -> None:
    """Record a STARTED + FAILED run event for a task that could not be resolved."""
    with integration_context(
        integration="managed-job",
        integration_pipeline=task_name,
        record_lifecycle=False,
    ) as ctx:
        await record_run_started(
            correlation=ctx.corelation,
            entry_point=task_name,
        )
        await record_run_ended(
            correlation=ctx.corelation,
            status="FAILED",
            attrs={"error": "Task not found"},
        )


async def _run_job_task(func: Any, task_name: str, args: list, kwargs: dict) -> bool:
    """Execute the resolved task within an integration_context. Returns True on success."""
    normalized_args = normalize_arguments(func, args, kwargs)

    with integration_context(
        integration="managed-job",
        integration_pipeline=task_name,
        record_lifecycle=False,
    ) as ctx:
        await record_run_started(
            correlation=ctx.corelation,
            entry_point=task_name,
            attrs={"args": normalized_args},
        )

        try:
            await _invoke_task_callable(func, args, kwargs)
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"Task execution failed: {task_name}: {e}", exc_info=True)
            await record_run_ended(
                correlation=ctx.corelation,
                status="FAILED",
                attrs={"error": str(e), "traceback": tb},
            )
            return False

        await record_run_ended(
            correlation=ctx.corelation,
            status="SUCCEEDED",
        )

    return True


# ─── register_schedules ────────────────────────────────────────────


def _cmd_register_schedules(deploy_id: str) -> None:
    """
    Sync feed consumers and register all locally registered scheduled tasks
    with the Managed Platform API: POST /v1/deploy/{deploy_id}/register
    """
    from flowstash.queue.backend import get_backend

    api_url, auth_token = _resolve_managed_api_context()

    # Always sync feed consumers first — even if there are no scheduled tasks.
    _cmd_sync_feed_consumers(api_url, auth_token)

    backend = get_backend()
    registered_tasks = getattr(backend, "_registered_tasks", [])

    if not registered_tasks:
        logger.info(
            "[register_schedules] No scheduled tasks registered — nothing to send."
        )

    tasks_payload = [
        {
            "task_id": t["task_id"],
            "task_name": t["task_name"],
            "integration": t.get("integration", "unknown"),
            "pipeline": t.get("pipeline", "unknown"),
            "default_schedule": t.get("default_schedule"),
        }
        for t in registered_tasks
    ]

    url = f"{api_url}/v1/deploy/{deploy_id}/register"
    logger.info(
        f"[register_schedules] Registering {len(tasks_payload)} task(s) → {url}"
    )

    try:
        with httpx.Client(
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        ) as client:
            response = client.post(url, json={"tasks": tasks_payload})
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            f"[register_schedules] API returned {e.response.status_code}: {e.response.text}"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"[register_schedules] Request failed: {e}")
        sys.exit(1)

    logger.info("[register_schedules] Registration successful.")
    sys.exit(0)


def _resolve_managed_api_context() -> tuple[str, str]:
    """Return (api_url, auth_token) from environment variables."""
    api_url = (
        os.getenv("FLOWSTASH_API_URL")
        or os.getenv("MANAGED_API_URL")
        or "https://api.flowstash.dev"
    ).rstrip("/")
    auth_token = os.getenv("MANAGED_AUTH_TOKEN", "")
    return api_url, auth_token


def _build_feed_consumers_payload() -> dict:
    """Build the full scoped snapshot payload from the local consumer registry."""
    consumers = get_registered_consumers()
    by_feed: dict[str, list] = defaultdict(list)
    for spec in consumers:
        by_feed[spec.feed_id].append(spec)

    return {
        "feeds": [
            {
                "feed_id": feed_id,
                "consumers": [
                    {
                        "group_name": s.subscription_name,
                        "batch": s.batch,
                        "max_batch_size": s.max_batch_size,
                        "max_delay_ms": s.max_delay_ms,
                        "debounce_delay_ms": s.debounce_delay_ms,
                        "max_debounce_window_ms": s.max_debounce_window_ms,
                    }
                    for s in specs
                ],
            }
            for feed_id, specs in by_feed.items()
        ]
    }


def sync_feed_consumers(
    api_url: str | None = None,
    auth_token: str | None = None,
    *,
    strict: bool = True,
) -> bool:
    """
    POST the full feed-consumer snapshot to the managed API.

    Returns True on success.  When *strict* is True a failure raises SystemExit(1);
    when *strict* is False a warning is logged and False is returned so the caller
    can decide whether to continue.
    """
    if api_url is None or auth_token is None:
        resolved_url, resolved_token = _resolve_managed_api_context()
        api_url = api_url or resolved_url
        auth_token = auth_token or resolved_token

    payload = _build_feed_consumers_payload()
    url = f"{api_url}/v1/feed/consumers/sync"
    logger.info(
        f"[sync_feed_consumers] Syncing {len(payload['feeds'])} feed(s) → {url}"
    )

    try:
        with httpx.Client(
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        ) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        msg = (
            f"[sync_feed_consumers] API returned {e.response.status_code}: "
            f"{e.response.text}"
        )
        if strict:
            logger.error(msg)
            sys.exit(1)
        logger.warning(msg)
        return False
    except Exception as e:
        msg = f"[sync_feed_consumers] Request failed: {e}"
        if strict:
            logger.error(msg)
            sys.exit(1)
        logger.warning(msg)
        return False

    logger.info("[sync_feed_consumers] Sync successful.")
    return True


def _cmd_sync_feed_consumers(api_url: str, auth_token: str) -> None:
    """Strict wrapper around sync_feed_consumers for job/CLI usage."""
    sync_feed_consumers(api_url=api_url, auth_token=auth_token, strict=True)


# ─── Consumer ────────────────────────────────────────────────────────


class ManagedConsumer(TaskConsumer):
    """
    TaskConsumer for managed / Cloud Run Job mode.

    Reads the 'run-task' command from sys.argv, resolves and executes
    the named task, flushes observability, and exits the process.
    """

    def __init__(self, config: RuntimeConfig):
        self.config = config

    async def start(self) -> None:
        argv = sys.argv[1:]

        if not argv:
            logger.error(
                "Managed consumer requires a command.\n"
                "  run-task <task_name> [args...]\n"
                "  register_schedules <deploy_id>"
            )
            sys.exit(1)

        command = argv[0]

        if command == "register_schedules":
            if len(argv) < 2:
                print(
                    "Usage: worker_main.py register_schedules <deploy_id>",
                    file=sys.stderr,
                )
                sys.exit(1)
            _cmd_register_schedules(deploy_id=argv[1])
            return  # sys.exit is called inside, but return for clarity

        if command != "run-task":
            logger.error(
                f"Unknown command: {command!r}. "
                "Commands: run-task <task_name> [args...] | register_schedules <deploy_id>"
            )
            sys.exit(1)

        task_name, args, kwargs = _parse_cli_args(argv[1:])
        logger.info(f"[run-task] Resolving task: {task_name!r}")

        try:
            func = resolve_function(task_name)
        except ValueError as e:
            logger.error(f"[run-task] {e}")
            await _record_missing_task(task_name)
            await asyncio.to_thread(AsyncManager.get_instance().flush, 15.0)
            sys.exit(1)

        logger.info(
            f"[run-task] Executing task: {task_name!r}, args={args}, kwargs={kwargs}"
        )
        success = await _run_job_task(func, task_name, args, kwargs)

        await asyncio.to_thread(AsyncManager.get_instance().flush, 15.0)
        sys.exit(0 if success else 1)

    async def stop(self) -> None:
        pass  # one-shot execution — nothing to stop

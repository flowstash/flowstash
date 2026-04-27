"""
HTTP Push Worker Entrypoint.

Receives task execution callbacks from Managed Scheduler via HTTP POST.
Deserializes the task payload, resolves the function, and executes it
within an IntegrationContext for observability.

Feed endpoints:
  POST /internal/feed/kick/batched   — batched consumer drain (called by Cloud Tasks)
  POST /internal/feed/deliver/classic — single-record classic delivery (called by Cloud Tasks)
"""
import importlib
import inspect
import logging
import os
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from flowstash.context import integration_context, current_context
from flowstash.observability.ingestion import (
    record_run_started,
    record_run_ended,
)
from flowstash.queue.backend import get_backend

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/schedules")
async def get_schedules():
    """
    Return all scheduled tasks registered in the active backend.

    Used by the Platform API deployment flow to discover schedules
    via a pull model instead of an INIT job push model.
    """
    backend = get_backend()

    # We expect ManagedTasksBackend to populate _registered_tasks
    if hasattr(backend, "_registered_tasks"):
        return {"tasks": backend._registered_tasks}

    return {"tasks": []}


# ─── Request Model ───────────────────────────────────────────────────


class TaskPayload(BaseModel):
    """Payload pushed by Managed Scheduler or Platform API."""
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    func_ref: Optional[str] = None  # e.g. "my_module.my_function"
    args: list = []
    kwargs: dict = {}
    integration: Optional[str] = None
    pipeline: Optional[str] = None
    triggered_by: Optional[str] = None
    cron: Optional[str] = None
    run_id: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None


# ─── Function Registry ───────────────────────────────────────────────


_task_registry: Dict[str, Any] = {}


def register_task(task_id: str, func: Any) -> None:
    """
    Register a callable task by its ID.

    Called during worker startup to build the function lookup table.
    """
    _task_registry[task_id] = func
    logger.info(f"Registered task handler: {task_id}")


def get_task_registry() -> Dict[str, Any]:
    """Return the current task registry."""
    return _task_registry


def _resolve_function(func_ref: str) -> Any:
    """
    Resolve a function reference like 'module.path.function_name' to a callable.

    First checks the local registry, then falls back to dynamic import.
    """
    # Check registry first
    if func_ref in _task_registry:
        return _task_registry[func_ref]

    # Dynamic import fallback
    parts = func_ref.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid function reference: {func_ref}")

    module_path, func_name = parts
    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        return func
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Cannot resolve function '{func_ref}': {e}")


# ─── Handler Endpoint ────────────────────────────────────────────────


@router.post("/handle_task")
async def handle_task(payload: TaskPayload):
    """
    Receive and execute a task triggered in managed mode.
    """
    func_ref = payload.func_ref or payload.task_id or payload.task_name
    
    if not func_ref:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'func_ref', 'task_id', or 'task_name' in payload",
        )

    # Resolve function
    try:
        func = _resolve_function(func_ref)
    except ValueError as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "TASK_NOT_FOUND", "detail": str(e)},
        )

    # Context Data
    integration = payload.integration or "unknown"
    pipeline = payload.pipeline or "unknown"
    run_id = payload.run_id
    tags = payload.tags or {}

    # Add trigger metadata
    if payload.triggered_by:
        tags["triggered_by"] = payload.triggered_by
    if payload.cron:
        tags["cron"] = payload.cron

    status_result = "SUCCEEDED"
    error = None

    with integration_context(
        integration=integration,
        integration_pipeline=pipeline,
        run_id=run_id,
        tags=tags,
    ) as ctx:
        await record_run_started(correlation=ctx.corelation)

        try:
            # Handle both TaskWrapper.run() and plain callables
            args = payload.args
            kwargs = payload.kwargs

            if hasattr(func, "run"):
                result = await func.run(*args, **kwargs)
            elif hasattr(func, "func"):
                # TaskWrapper — call the underlying func
                underlying = func.func
                if inspect.iscoroutinefunction(underlying):
                    result = await underlying(*args, **kwargs)
                else:
                    result = underlying(*args, **kwargs)
            else:
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

        except Exception as e:
            status_result = "FAILED"
            error = str(e)
            logger.error(f"Task execution failed: {func_ref}: {e}", exc_info=True)
            await record_run_ended(
                status=status_result,
                correlation=ctx.corelation,
            )
            # Return 200 with FAILED status so scheduler (e.g. QStash) does not retry unboundedly
            return {
                "status": "FAILED",
                "error": error,
                "task": func_ref,
            }

        await record_run_ended(
            status=status_result,
            correlation=ctx.corelation,
        )

    return {
        "status": "ok",
        "task": func_ref,
    }


# ─── Feed: shared helpers ────────────────────────────────────────────


def _get_api_client_config() -> tuple[str, str]:
    """
    Return (api_url, auth_token) for calling the Managed Platform API.
    Reads from environment (set by initialize_runtime).
    """
    api_url = (
        os.getenv("FLOWSTASH_API_URL")
        or os.getenv("MANAGED_API_URL")
        or "https://api.flowstash.dev"
    ).rstrip("/")
    auth_token = os.getenv("MANAGED_AUTH_TOKEN", "")
    return api_url, auth_token


async def _resolve_record_from_item(item: dict) -> Optional[Any]:
    """
    Build a RecordData from a buffer item dict.

    Handles blob_ref resolution (F5): if the item has a blob_ref, fetch the
    actual data from BlobStore before constructing the RecordData.
    """
    from flowstash.pipelines.records_model import RecordData

    data = item.get("data")
    blob_ref = item.get("blob_ref")

    if blob_ref and data is None:
        # Resolve blob from BlobStore
        try:
            import json as _json
            import asyncio
            from flowstash.observability.registry import get_blob_store
            blob_bytes = await asyncio.to_thread(
                get_blob_store().get, blob_ref
            )
            data = _json.loads(blob_bytes)
        except Exception as e:
            logger.error(
                f"Failed to resolve blob_ref={blob_ref} for "
                f"dedupe_key={item.get('dedupe_key')}: {e}"
            )
            return None  # item cannot be delivered without its data

    ts = item.get("timestamp")
    return RecordData(
        record_id=item.get("record_id") or item.get("dedupe_key"),
        record_type=item.get("record_type") or "managed",
        data=data,
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else None,
        dedupe_key=item.get("dedupe_key"),
    )


# ─── Feed: Batched kick ──────────────────────────────────────────────


class KickBatchedRequest(BaseModel):
    tenant_id: str
    feed_id: str
    group_name: str


@router.post("/internal/feed/kick/batched")
async def kick_batched(request: KickBatchedRequest):
    """
    Called by Cloud Tasks to trigger batched feed consumption.

    Flow:
      1. Lease a batch from Managed API
      2. Resolve blob refs for items where data is in BlobStore (F5)
      3. Invoke the local @feed_consumer handler
      4. ACK results back to Managed API
         - 200 always if we reach /ack (Cloud Tasks won't retry double-delivery)
         - 500 only if we couldn't reach /ack (so Cloud Tasks retries the kick)
    """
    api_url, auth_token = _get_api_client_config()

    try:
        async with httpx.AsyncClient(
            base_url=api_url,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=60.0,
        ) as client:

            # 1. Lease a batch
            resp = await client.post(
                f"/v1/feed/{request.feed_id}/lease",
                json={"group_name": request.group_name, "max_batch_size": 100},
            )
            resp.raise_for_status()
            lease_data = resp.json()

            run_id = lease_data.get("run_id")
            items = lease_data.get("items", [])

            if not items:
                logger.info(
                    f"kick_batched: no items for feed={request.feed_id} "
                    f"group={request.group_name} — nothing to do"
                )
                return {"status": "ok", "message": "no items"}

            # 2. Find the matching consumer handler
            from flowstash.pipelines.consumer import get_registered_consumers
            consumers = get_registered_consumers()
            spec = next(
                (c for c in consumers if c.subscription_name == request.group_name),
                None,
            )

            success_keys: List[str] = []
            failed_keys: List[str] = []
            failure_reason: Optional[str] = None

            if not spec:
                logger.warning(
                    f"kick_batched: no handler registered for group={request.group_name}"
                )
                failed_keys = [it["dedupe_key"] for it in items]
                failure_reason = f"No handler registered for group '{request.group_name}'"
            else:
                # 3. Resolve records (including any blob_ref fetches — F5)
                import asyncio
                records_or_none = await asyncio.gather(
                    *[_resolve_record_from_item(it) for it in items]
                )
                records = [r for r in records_or_none if r is not None]
                unresolvable = [
                    it["dedupe_key"]
                    for it, r in zip(items, records_or_none)
                    if r is None
                ]
                if unresolvable:
                    failed_keys.extend(unresolvable)
                    failure_reason = "Blob resolution failed for some items"

                if records:
                    try:
                        # Deliver to handler
                        with integration_context(
                            tenant_id=request.tenant_id,
                            integration=request.feed_id,
                            integration_pipeline=request.group_name,
                            run_id=run_id,
                        ):
                            if spec.batch:
                                await spec.handler(records)
                            else:
                                for r in records:
                                    await spec.handler(r)
                        success_keys = [r.dedupe_key for r in records if r.dedupe_key]
                    except Exception as e:
                        logger.error(
                            f"Consumer handler failed for group={request.group_name}: {e}",
                            exc_info=True,
                        )
                        failed_keys.extend([r.dedupe_key for r in records if r.dedupe_key])
                        failure_reason = str(e)

            # 4. ACK results — must reach this point; return 500 if we can't
            ack_resp = await client.post(
                f"/v1/feed/{request.feed_id}/ack",
                json={
                    "group_name": request.group_name,
                    "run_id": run_id,
                    "success_keys": success_keys,
                    "failed_keys": failed_keys,
                    "failure_reason": failure_reason,
                },
            )
            ack_resp.raise_for_status()

    except httpx.HTTPStatusError as e:
        # If /lease or /ack returned a server error, return 500 so Cloud Tasks retries kick
        logger.error(f"kick_batched HTTP error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"kick_batched unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok"}


# ─── Feed: Classic delivery ──────────────────────────────────────────


class DeliverClassicRequest(BaseModel):
    """
    Pushed by Cloud Tasks when a classic (single-record, non-batched) consumer
    is registered. Contains one complete record.
    """
    tenant_id: str
    feed_id: str
    group_name: str
    # Record fields — mirrors what publish stores
    dedupe_key: str
    timestamp: float
    data: Optional[Any] = None
    blob_ref: Optional[str] = None
    record_type: Optional[str] = "managed"
    record_id: Optional[str] = None


@router.post("/internal/feed/deliver/classic")
async def deliver_classic(request: DeliverClassicRequest):
    """
    Called by Cloud Tasks to deliver a single record to a classic (non-batched)
    @feed_consumer handler.

    Unlike batched kick, there is no lease/ack cycle — the record is delivered
    directly. Cloud Tasks handles retries via non-2xx returns.
    """
    from flowstash.pipelines.consumer import get_registered_consumers

    # Find matching handler
    consumers = get_registered_consumers()
    spec = next(
        (c for c in consumers if c.subscription_name == request.group_name),
        None,
    )

    if not spec:
        logger.warning(
            f"deliver_classic: no handler for group={request.group_name}. "
            "Returning 404 so Cloud Tasks stops retrying."
        )
        raise HTTPException(
            status_code=404,
            detail=f"No handler registered for group '{request.group_name}'",
        )

    # Build the record (with blob resolution)
    item = {
        "dedupe_key": request.dedupe_key,
        "timestamp": request.timestamp,
        "data": request.data,
        "blob_ref": request.blob_ref,
        "record_type": request.record_type,
        "record_id": request.record_id,
    }
    record = await _resolve_record_from_item(item)
    if record is None:
        # Blob resolution failed — return 500 so Cloud Tasks retries
        raise HTTPException(
            status_code=500,
            detail=f"Could not resolve data for dedupe_key={request.dedupe_key}",
        )

    try:
        with integration_context(
            tenant_id=request.tenant_id,
            integration=request.feed_id,
            integration_pipeline=request.group_name,
        ):
            if spec.batch:
                # Handler declared batch=True but received via classic path — wrap in list
                await spec.handler([record])
            else:
                await spec.handler(record)
    except Exception as e:
        logger.error(
            f"deliver_classic handler failed for group={request.group_name}: {e}",
            exc_info=True,
        )
        # Return 500 so Cloud Tasks retries
        raise HTTPException(
            status_code=500,
            detail=f"Handler execution failed: {e}",
        )

    return {"status": "ok", "dedupe_key": request.dedupe_key}

"""
HTTP Push Worker Entrypoint.

Receives task execution callbacks from Managed Scheduler via HTTP POST.
Deserializes the task payload, resolves the function, and executes it
within an IntegrationContext for observability.

Feed endpoints:
  POST /internal/feed/kick/batched   — batched consumer drain (called by Cloud Tasks)
  POST /internal/feed/deliver/classic — single-record classic delivery (called by Cloud Tasks)
"""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from flowstash.context import integration_context
from flowstash.observability.ingestion import (
    record_run_started,
    record_run_ended,
    normalize_arguments,
)

from .drain import ManagedTaskDrainController
from .task_resolver import (
    resolve_function as _registry_resolve,
    _invoke_task_callable,
)
from .lease_client import (
    get_lease_client,
    LeaseBusy,
    SUCCEEDED as LEASE_SUCCEEDED,
    FAILED as LEASE_FAILED,
)
from flowstash.pipelines.record_serialization import from_jsonable

logger = logging.getLogger(__name__)

router = APIRouter()


# ─── Request Models ─────────────────────────────────────────────────


class DelegationMetadataModel(BaseModel):
    """Causal envelope propagated from the triggering run to this execution."""

    parent_run_id: Optional[str] = None
    operation_id: Optional[str] = None
    target_task: Optional[str] = None
    accepted_id: Optional[str] = None
    schedule_time: Optional[str] = None
    attrs: Dict[str, Any] = Field(default_factory=dict)


class ManagedHandleTaskPayload(BaseModel):
    """Legacy nested payload accepted for backward compatibility."""

    func_ref: Optional[str] = None
    args: list = Field(default_factory=list)
    kwargs: dict = Field(default_factory=dict)
    integration: Optional[str] = None
    pipeline: Optional[str] = None
    triggered_by: Optional[str] = None
    cron: Optional[str] = None
    run_id: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    delegation: Optional[DelegationMetadataModel] = None


class TaskPayload(BaseModel):
    """Payload pushed by Managed Scheduler or Platform API."""

    task_id: Optional[str] = None
    task_name: Optional[str] = None
    func_ref: Optional[str] = None
    args: list = Field(default_factory=list)
    kwargs: dict = Field(default_factory=dict)
    integration: Optional[str] = None
    pipeline: Optional[str] = None
    triggered_by: Optional[str] = None
    cron: Optional[str] = None
    # run_id is pre-allocated by the platform at submit time and travels in BOTH the
    # payload and the request URL (?run_id=...). The execution side USES it as the run_id
    # so the id is known up-front and is STABLE across Cloud Tasks redeliveries — that lets
    # Cloud Run failure logs (which include the URL's run_id) be joined back to this run.
    # Older payloads that omit it fall back to a freshly generated id. Causal metadata is
    # carried in the `delegation` field instead.
    run_id: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    delegation: Optional[DelegationMetadataModel] = None
    payload: Optional[ManagedHandleTaskPayload] = None

    def _payload_value(self, field_name: str) -> Any:
        if self.payload is None:
            return None
        return getattr(self.payload, field_name)

    def effective_func_ref(self) -> Optional[str]:
        return self.func_ref or self._payload_value("func_ref")

    def effective_args(self) -> list:
        return self.args or self._payload_value("args") or []

    def effective_kwargs(self) -> dict:
        return self.kwargs or self._payload_value("kwargs") or {}

    def effective_integration(self) -> Optional[str]:
        return self.integration or self._payload_value("integration")

    def effective_pipeline(self) -> Optional[str]:
        return self.pipeline or self._payload_value("pipeline")

    def effective_triggered_by(self) -> Optional[str]:
        return self.triggered_by or self._payload_value("triggered_by")

    def effective_cron(self) -> Optional[str]:
        return self.cron or self._payload_value("cron")

    def effective_tags(self) -> Dict[str, Any]:
        tags = dict(self._payload_value("tags") or {})
        tags.update(self.tags or {})
        return tags

    def effective_delegation(self) -> Optional[DelegationMetadataModel]:
        return self.delegation or self._payload_value("delegation")

    def execution_task_ref(self) -> Optional[str]:
        return self.task_id or self.task_name or self.effective_func_ref()


# ─── Task Resolution ─────────────────────────────────────────────────


def _resolve_task_callable(task_name: str, func_ref: Optional[str]) -> Any:
    """
    Resolve a task callable.

    Tries the task registry by task_name first, then falls back to func_ref
    (dotted import path) for backward compatibility.
    Raises ValueError if the task cannot be resolved.
    """
    try:
        if task_name:
            return _registry_resolve(task_name)
    except ValueError:
        pass

    if func_ref:
        try:
            return _registry_resolve(func_ref)
        except ValueError:
            pass

    raise ValueError(f"Cannot resolve task '{task_name}'")


def _get_drain_controller(request: Request) -> ManagedTaskDrainController:
    controller = getattr(request.app.state, "managed_task_drain_controller", None)
    if controller is None:
        raise RuntimeError("Managed drain controller is not configured")
    return controller


async def _flush_observability(timeout_s: float = 30.0) -> None:
    import asyncio
    from flowstash.observability.ingestion import AsyncManager

    await asyncio.to_thread(AsyncManager.get_instance().flush, timeout_s)


@asynccontextmanager
async def _managed_request_scope(request: Request, kind: str, task_ref: str):
    controller = _get_drain_controller(request)
    admitted = await controller.begin_request(kind, task_ref)
    if not admitted:
        yield False
        return

    try:
        yield True
    finally:
        await controller.finish_request(kind, task_ref)
        await _flush_observability()


async def _execute_managed_task(payload: TaskPayload) -> dict:
    integration = payload.effective_integration() or "unknown"
    pipeline = payload.effective_pipeline() or "unknown"
    parent_run_id: Optional[str] = None
    operation_id: Optional[str] = None
    delegation = payload.effective_delegation()
    if delegation:
        parent_run_id = delegation.parent_run_id
        operation_id = delegation.operation_id
    tags = payload.effective_tags()

    triggered_by = payload.effective_triggered_by()
    if triggered_by:
        tags["triggered_by"] = triggered_by

    cron = payload.effective_cron()
    if cron:
        tags["cron"] = cron

    func_ref = payload.effective_func_ref() or payload.task_id
    entry_point = payload.task_name or func_ref or payload.task_id or "unknown"
    args = payload.effective_args()
    kwargs = payload.effective_kwargs()
    raw_args = {"args": args, "kwargs": kwargs}

    # Use the run_id pre-allocated by the platform (carried in the payload and the
    # request URL). It is known up-front and STABLE across Cloud Tasks redeliveries, so
    # all retries of one task share a run_id (collapsing to a single run) and Cloud Run
    # failure logs — which include the URL's run_id — can be joined back to this run.
    # Fall back to a fresh id for older payloads that don't carry one.
    execution_run_id = payload.run_id or str(uuid.uuid4())

    # Idempotency guard: claim an exclusive lease on this run_id from the broker so a
    # Cloud Tasks redelivery (or a concurrent duplicate) never re-executes a run that is
    # already running or just completed. Disabled-safe: get_lease_client() returns None
    # when the broker isn't configured; an unreachable broker yields UNAVAILABLE → we
    # fail closed with a retryable 503 rather than risk a duplicate.
    lease = get_lease_client()
    lease_held = False
    if lease is not None:
        res = await lease.acquire(execution_run_id, entry_point=entry_point)
        if res.duplicate:
            logger.info(
                "run %s already completed — skipping redelivery", execution_run_id
            )
            return {"status": "duplicate", "run_id": execution_run_id, "task": func_ref}
        if not res.acquired:
            # BUSY (held elsewhere), RECOVERING (broker restarting), or UNAVAILABLE
            # (broker unreachable) — all map to a retryable 503. Only ACQUIRED runs.
            raise LeaseBusy(execution_run_id)
        lease_held = True

    lease_status = LEASE_FAILED
    try:
        with integration_context(
            integration=integration,
            integration_pipeline=pipeline,
            run_id=execution_run_id,
            parent_run_id=parent_run_id,
            operation_id=operation_id,
            tags=tags,
            attrs={"args": raw_args},
            record_lifecycle=False,
        ) as ctx:
            try:
                func = _resolve_task_callable(
                    payload.task_name or payload.task_id, func_ref
                )
            except ValueError as e:
                logger.warning(f"Could not resolve task '{entry_point}': {e}")
                await record_run_started(
                    correlation=ctx.corelation,
                    entry_point=entry_point,
                    attrs={"args": raw_args},
                )
                await record_run_ended(
                    status="FAILED",
                    correlation=ctx.corelation,
                    attrs={"error": str(e), "task_resolution_failed": True},
                )
                raise

            normalized_args = normalize_arguments(func, args, kwargs)
            await record_run_started(
                correlation=ctx.corelation,
                entry_point=entry_point,
                attrs={"args": normalized_args},
            )

            try:
                await _invoke_task_callable(func, args, kwargs)
            except Exception as e:
                import traceback

                error = str(e)
                tb = traceback.format_exc()
                logger.error(f"Task execution failed: {func_ref}: {e}", exc_info=True)
                await record_run_ended(
                    status="FAILED",
                    correlation=ctx.corelation,
                    attrs={"error": error, "traceback": tb},
                )
                # Re-raise so the run ends as FAILED only (not ALSO SUCCEEDED) and the handler
                # returns 500 → Cloud Tasks retries the delivery. Without this, a raising task
                # was recorded as both FAILED and SUCCEEDED and the worker returned 200.
                raise

            await record_run_ended(
                status="SUCCEEDED",
                correlation=ctx.corelation,
            )
            lease_status = LEASE_SUCCEEDED
    finally:
        if lease_held:
            # SUCCEEDED → completed-set tombstone (skips post-completion redeliveries);
            # FAILED → free the lease so Cloud Tasks can genuinely retry.
            await lease.release(execution_run_id, lease_status)

    return {
        "status": "ok",
        "task": func_ref,
    }


# ─── Handler Endpoint ────────────────────────────────────────────────


@router.post("/handle_task")
async def handle_task(request: Request, payload: TaskPayload):
    """
    Receive and execute a task delivered by the managed platform.

    Accepts the original flat task payload. A nested `payload` object is still
    accepted for backward compatibility with already-enqueued deliveries.
    Returns 404 + {"status": "TASK_NOT_FOUND"} when the task cannot be resolved
    (signals the platform to clean up orphaned schedules).
    Returns 500 on execution failure so Cloud Tasks retries the delivery.
    """
    task_ref = payload.execution_task_ref()
    if not task_ref:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'task_id', 'task_name', or 'func_ref' in payload",
        )

    async with _managed_request_scope(request, "task", task_ref) as admitted:
        if not admitted:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "DRAINING", "task": task_ref},
            )

        try:
            return await _execute_managed_task(payload)
        except ValueError as e:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"status": "TASK_NOT_FOUND", "detail": str(e)},
            )
        except LeaseBusy as e:
            # Lease held elsewhere (or broker unreachable). 503 is retryable by
            # Cloud Tasks (unlike a 4xx), so the redelivery becomes the liveness
            # backstop: by the next attempt the holder has finished (→ COMPLETED)
            # or its lease expired (→ this attempt acquires it).
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "LEASE_HELD", "run_id": e.run_id},
            )
        except Exception as e:
            # Task execution failed (already recorded as FAILED on the run). Return 500 so
            # Cloud Tasks retries the delivery; the request scope still releases its slot
            # and flushes observability in its finally block.
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": {"status": "FAILED", "error": str(e)}},
            )


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

            blob_bytes = await asyncio.to_thread(get_blob_store().get, blob_ref)
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
        data=from_jsonable(data),
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else None,
        dedupe_key=item.get("dedupe_key"),
    )


# ─── Feed: Batched kick ──────────────────────────────────────────────


class KickBatchedRequest(BaseModel):
    tenant_id: str
    feed_id: str
    group_name: str


@router.post("/internal/feed/kick/batched")
async def kick_batched(http_request: Request, request: KickBatchedRequest):
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
    async with _managed_request_scope(
        http_request,
        "feed-kick",
        f"{request.feed_id}:{request.group_name}",
    ) as admitted:
        if not admitted:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "DRAINING",
                    "feed_id": request.feed_id,
                    "group_name": request.group_name,
                },
            )

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
                    (
                        c
                        for c in consumers
                        if c.feed_id == request.feed_id
                        and c.subscription_name == request.group_name
                    ),
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
                    failure_reason = (
                        f"No handler registered for group '{request.group_name}'"
                    )
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
                            # Deliver to handler.
                            # The integration_context wraps both handler execution AND the
                            # subsequent /ack call so the run ends only after the lease is
                            # durably committed. run_id comes from the managed API lease.
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

                                # 4. ACK results — inside the run context so the run ends
                                #    only after the lease is durably committed.
                                success_keys = [
                                    r.dedupe_key for r in records if r.dedupe_key
                                ]
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

                        except Exception as e:
                            logger.error(
                                f"Consumer handler failed for group={request.group_name}: {e}",
                                exc_info=True,
                            )
                            failed_keys.extend(
                                [r.dedupe_key for r in records if r.dedupe_key]
                            )
                            failure_reason = str(e)
                            # ACK with failures so the lease is released even on handler error.
                            ack_resp = await client.post(
                                f"/v1/feed/{request.feed_id}/ack",
                                json={
                                    "group_name": request.group_name,
                                    "run_id": run_id,
                                    "success_keys": [],
                                    "failed_keys": failed_keys,
                                    "failure_reason": failure_reason,
                                },
                            )
                            ack_resp.raise_for_status()
                    return {
                        "status": "ok",
                        "message": f"processed {len(records)} records",
                    }

            # Handler was missing or all items unresolvable — ACK failures without a run context.
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
    is registered. Contains one complete record plus causal source metadata.
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
    # Causal source metadata from the publishing run
    source_integration: Optional[str] = None
    source_pipeline: Optional[str] = None
    source_run_id: Optional[str] = None
    source_traceparent: Optional[str] = None


@router.post("/internal/feed/deliver/classic")
async def deliver_classic(http_request: Request, request: DeliverClassicRequest):
    """
    Called by Cloud Tasks to deliver a single record to a classic (non-batched)
    @feed_consumer handler.

    Unlike batched kick, there is no lease/ack cycle — the record is delivered
    directly. Cloud Tasks handles retries via non-2xx returns.
    """
    async with _managed_request_scope(
        http_request,
        "feed-deliver",
        f"{request.feed_id}:{request.dedupe_key}",
    ) as admitted:
        if not admitted:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "DRAINING",
                    "feed_id": request.feed_id,
                    "dedupe_key": request.dedupe_key,
                },
            )

        from flowstash.pipelines.consumer import get_registered_consumers

        consumers = get_registered_consumers()
        spec = next(
            (
                c
                for c in consumers
                if c.feed_id == request.feed_id
                and c.subscription_name == request.group_name
            ),
            None,
        )

        if not spec:
            logger.warning(
                f"deliver_classic: no handler for feed={request.feed_id} "
                f"group={request.group_name}. "
                "Returning 404 so Cloud Tasks stops retrying."
            )
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No handler registered for feed '{request.feed_id}' "
                    f"group '{request.group_name}'"
                ),
            )

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
            raise HTTPException(
                status_code=500,
                detail=f"Could not resolve data for dedupe_key={request.dedupe_key}",
            )

        source_tags: Dict[str, Any] = {}
        if request.source_integration:
            source_tags["source_integration"] = request.source_integration
        if request.source_pipeline:
            source_tags["source_pipeline"] = request.source_pipeline
        if request.source_run_id:
            source_tags["source_run_id"] = request.source_run_id
        if request.source_traceparent:
            source_tags["source_traceparent"] = request.source_traceparent

        try:
            with integration_context(
                tenant_id=request.tenant_id,
                integration=request.feed_id,
                integration_pipeline=request.group_name,
                tags=source_tags,
            ):
                if spec.batch:
                    await spec.handler([record])
                else:
                    await spec.handler(record)
        except Exception as e:
            logger.error(
                f"deliver_classic handler failed for feed={request.feed_id} "
                f"group={request.group_name}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Handler execution failed: {e}",
            )

        return {"status": "ok", "dedupe_key": request.dedupe_key}

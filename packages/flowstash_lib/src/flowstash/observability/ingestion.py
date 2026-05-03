import asyncio
import logging
import threading
import uuid
import concurrent.futures
from datetime import datetime, UTC
from typing import Callable, Coroutine, Optional, Any, Dict

from opentelemetry import trace

from ..context import current_context
from .model import (
    Correlation,
    Run,
    Span,
    DataExchange,
    Channel,
    State,
    RunEvent,
    SpanEvent,
    DataExchangeEvent,
    RecordLink,
    RecordLinkKind,
)
from .registry import (
    get_events_store,
    get_data_exchange_store,
    get_blob_store,
    get_records_store,
    configure,
)


from ..config.observability_config import DurabilityMode, ObservabilityConfig

logger = logging.getLogger(__name__)

_config = ObservabilityConfig()


def set_observability_config(config: ObservabilityConfig):
    global _config
    _config = config
    configure(config)

    try:
        from .logging import setup_global_logging

        setup_global_logging()
    except Exception as e:
        logger.warning(f"Failed to setup global logging capture: {e}")


class AsyncManager:
    _instance = None

    def __init__(self, max_workers: int = 4):
        self._max_workers = max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._io_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        )
        self._pending: list[concurrent.futures.Future] = []
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AsyncManager()
        return cls._instance

    def _submit(self, fn: Callable) -> concurrent.futures.Future:
        """Submit fn to executor and track the future for flush()."""
        future = self._executor.submit(fn)
        with self._lock:
            self._pending = [f for f in self._pending if not f.done()]
            self._pending.append(future)
        return future

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all pending observability work completes (or timeout expires).

        Three-level drain:
          1. _executor trampoline threads (lifecycle coroutine runners).
          2. _io_executor store-write threads submitted via execute().
          3. The store's own internal delivery queue (e.g. ApiEventsStore HTTP queue).

        Safe to call from any thread (sync). Called during graceful shutdown.
        """
        # Level 1: wait for lifecycle trampoline threads
        with self._lock:
            pending = list(self._pending)
        if pending:
            concurrent.futures.wait(pending, timeout=timeout)
        with self._lock:
            self._pending.clear()

        # Level 2: wait for any outstanding _io_executor jobs (store writes)
        self._io_executor.shutdown(wait=True, cancel_futures=False)
        self._io_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_workers
        )

        # Level 3: drain the store's own internal queue (e.g. HTTP delivery queue)
        try:
            store = get_events_store()
            if hasattr(store, "flush"):
                store.flush(timeout=timeout)
        except Exception:
            pass

    async def execute(self, func, *args, **kwargs):
        """Execute a blocking function in the thread pool, respecting durability config."""

        loop = asyncio.get_running_loop()

        # Wrap the call to handle kwargs which run_in_executor doesn't support directly
        def _job():
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Observability async job failed: {e}", exc_info=True)

        if _config.durability == DurabilityMode.IMMEDIATE:
            # Await the execution in the thread pool
            await loop.run_in_executor(self._io_executor, _job)
        else:
            # EVENTUAL: Fire and forget task that wraps calculation
            async def _background():
                await loop.run_in_executor(self._io_executor, _job)

            asyncio.create_task(_background())

    def execute_fire_and_forget(self, func, *args, **kwargs):
        """Execute a blocking function in the thread pool. Tracked by flush()."""

        def _job():
            try:
                func(*args, **kwargs)
            except Exception:
                # Swallow all to prevent recursive logging/failures
                pass

        self._submit(_job)


def _enqueue_lifecycle(coro_fn: Callable[..., Coroutine], *args, **kwargs) -> None:
    """Submit an async lifecycle call to the thread pool. Fire-and-forget, drain-safe.
    Creates a private event loop per invocation so callers need no running loop."""

    def _run():
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro_fn(*args, **kwargs))
            # Drain any tasks created during the coroutine (handles EVENTUAL durability mode)
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
        except Exception as e:
            logger.debug(f"Lifecycle event suppressed: {e}")
        finally:
            loop.close()

    AsyncManager.get_instance()._submit(_run)


def _get_correlation(correlation: Optional[Correlation] = None) -> Correlation:
    if correlation:
        return correlation
    ctx = current_context()
    if ctx:
        return ctx.corelation
    return Correlation()


# --- Run Events ---


async def record_run_started(
    artifact_id: Optional[str] = None,
    env_snapshot_id: Optional[str] = None,
    correlation: Optional[Correlation] = None,
    started_at: Optional[datetime] = None,
    status: str = "RUNNING",
    scheduled_job_id: Optional[str] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    ctx = current_context()
    run_attrs = {**(attrs or {})}
    if not run_attrs.get("fw.record_key") and ctx and ctx.current_record_key:
        run_attrs["fw.record_key"] = ctx.current_record_key

    event = RunEvent(
        event_type="STARTED",
        correlation=corr,
        occurred_at=started_at or datetime.now(UTC),
        scheduled_job_id=scheduled_job_id,
        artifact_id=artifact_id,
        env_snapshot_id=env_snapshot_id,
        status=status,
        attrs=run_attrs,
    )
    await AsyncManager.get_instance().execute(get_events_store().write_run_event, event)


async def record_run_progress(
    status: str = "RUNNING",
    correlation: Optional[Correlation] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    event = RunEvent(
        event_type="PROGRESS", correlation=corr, status=status, attrs=attrs or {}
    )
    await AsyncManager.get_instance().execute(get_events_store().write_run_event, event)


async def record_run_ended(
    status: str = "SUCCEEDED",
    correlation: Optional[Correlation] = None,
    finished_at: Optional[datetime] = None,
    scheduled_job_id: Optional[str] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    ctx = current_context()
    run_attrs = {**(attrs or {})}
    if not run_attrs.get("fw.record_key") and ctx and ctx.current_record_key:
        run_attrs["fw.record_key"] = ctx.current_record_key

    event = RunEvent(
        event_type="ENDED",
        correlation=corr,
        occurred_at=finished_at or datetime.now(UTC),
        finished_at=finished_at or datetime.now(UTC),
        scheduled_job_id=scheduled_job_id,
        status=status,
        attrs=run_attrs,
    )
    await AsyncManager.get_instance().execute(get_events_store().write_run_event, event)


async def record_run_scheduled(
    correlation: Optional[Correlation] = None,
    scheduled_job_id: Optional[str] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    ctx = current_context()
    run_attrs = {**(attrs or {})}
    if not run_attrs.get("fw.record_key") and ctx and ctx.current_record_key:
        run_attrs["fw.record_key"] = ctx.current_record_key

    event = RunEvent(
        event_type="SCHEDULED",
        correlation=corr,
        status="SCHEDULED",
        scheduled_job_id=scheduled_job_id,
        attrs=run_attrs,
    )
    await AsyncManager.get_instance().execute(get_events_store().write_run_event, event)


# --- Span Events ---


async def record_span_started(
    name: str,
    correlation: Optional[Correlation] = None,
    status: str = "STARTED",
    start_time: Optional[datetime] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    ctx = current_context()
    span_attrs = {**(attrs or {})}
    if not span_attrs.get("fw.record_key") and ctx and ctx.current_record_key:
        span_attrs["fw.record_key"] = ctx.current_record_key

    event = SpanEvent(
        event_type="STARTED",
        correlation=corr,
        occurred_at=start_time or datetime.now(UTC),
        name=name,
        status=status,
        start_time=start_time,
        attrs=span_attrs,
    )
    await AsyncManager.get_instance().execute(
        get_events_store().write_span_event, event
    )


async def record_span_progress(
    name: str,
    correlation: Optional[Correlation] = None,
    status: str = "RUNNING",
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    event = SpanEvent(
        event_type="PROGRESS",
        correlation=corr,
        name=name,
        status=status,
        attrs=attrs or {},
    )
    await AsyncManager.get_instance().execute(
        get_events_store().write_span_event, event
    )


async def record_span_ended(
    name: str,
    correlation: Optional[Correlation] = None,
    status: str = "OK",
    error_summary: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    ctx = current_context()
    span_attrs = {**(attrs or {})}
    if not span_attrs.get("fw.record_key") and ctx and ctx.current_record_key:
        span_attrs["fw.record_key"] = ctx.current_record_key

    event = SpanEvent(
        event_type="ENDED",
        correlation=corr,
        occurred_at=end_time or datetime.now(UTC),
        name=name,
        status=status,
        error_summary=error_summary,
        start_time=start_time,
        end_time=end_time or datetime.now(UTC),
        attrs=span_attrs,
    )
    await AsyncManager.get_instance().execute(
        get_events_store().write_span_event, event
    )


# --- Logs ---


async def record_log(
    message: str,
    severity: str = "INFO",
    correlation: Optional[Correlation] = None,
    attrs: Optional[Dict[str, Any]] = None,
):
    corr = _get_correlation(correlation)
    await AsyncManager.get_instance().execute(
        get_events_store().write_log, corr, severity, message, attrs or {}
    )


def _resolve_callable(path: str):
    import importlib

    if not path:
        return None
    try:
        module_path, attr_name = path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        return getattr(mod, attr_name)
    except (ImportError, AttributeError, ValueError):
        return None


def enqueue_log_event(
    logger_name: str,
    levelno: int,
    message: str,
    attrs: Optional[Dict[Any, Any]] = None,
    exc_info: bool = False,
):
    """
    Called by IntegrationLogger to capture log events in-context.
    Swallows all exceptions.
    """
    try:
        ctx = current_context()
        if ctx is None:
            return

        log_cfg = _config.logging
        if not log_cfg.enabled:
            return

        # Recursion safety
        if logger_name.startswith("flowstash.observability"):
            return

        # Level check
        min_level = log_cfg.min_level
        if isinstance(min_level, str):
            min_level_no = getattr(logging, min_level, logging.INFO)
        else:
            min_level_no = min_level

        if levelno < min_level_no:
            return

        # Prefix checks
        if log_cfg.include_prefixes:
            if not any(logger_name.startswith(p) for p in log_cfg.include_prefixes):
                return

        if log_cfg.exclude_prefixes:
            if any(logger_name.startswith(p) for p in log_cfg.exclude_prefixes):
                return

        # Filter fn
        if log_cfg.filter_fn:
            filter_fn = _resolve_callable(log_cfg.filter_fn)
            if filter_fn:
                level_name = logging.getLevelName(levelno)
                record_like = {
                    "logger_name": logger_name,
                    "levelno": levelno,
                    "level": level_name,
                    "message": message,
                    "attrs": attrs,
                    "exc_info": exc_info,
                }
                try:
                    if not filter_fn(record_like, ctx):
                        return
                except Exception:
                    # Allow on error
                    pass

        # Prepare for storage
        corr = ctx.corelation
        log_attrs = {**(attrs or {})}
        if exc_info:
            log_attrs["exc_info"] = True
        log_attrs["logger_name"] = logger_name

        # Add record correlation
        record_key = attrs.get("fw.record_key") if attrs else None
        if not record_key and ctx.current_record_key:
            record_key = ctx.current_record_key
        if record_key:
            log_attrs["fw.record_key"] = record_key

        level_name = logging.getLevelName(levelno)

        # Enqueue for storage
        AsyncManager.get_instance().execute_fire_and_forget(
            get_events_store().write_log, corr, level_name, message, log_attrs
        )
    except Exception:
        # Observability must never fail execution
        pass


async def enqueue_record_link(
    tenant_id: str,
    integration: Optional[str],
    pipeline: Optional[str],
    run_id: str,
    record_key: str,
    kind: RecordLinkKind,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None,
    source: Optional[str] = None,
):
    """
    Emit a RecordLink event.
    """
    link = RecordLink(
        tenant_id=tenant_id,
        integration=integration,
        pipeline=pipeline,
        run_id=run_id,
        trace_id=trace_id,
        span_id=span_id,
        record_key=record_key,
        kind=kind,
        source=source,
        event_time=datetime.now(UTC),
    )

    # We need a records store
    try:
        store = get_records_store()
        await AsyncManager.get_instance().execute(store.write_record_link, link)
    except Exception:
        # Never fail
        pass


# --- Data Exchange ---


def _emit_otel_data_exchange_event(
    event: DataExchangeEvent, dx_id: str, corr: Correlation
):
    """
    Emit an OTEL span event (not a full span) with data exchange information.
    This adds the data exchange details as an event on the current span,
    so OTEL consumers see the same info without creating separate HTTP spans.
    """
    current_span = trace.get_current_span()
    if current_span is None or not current_span.is_recording():
        return

    # Build event attributes from the data exchange info
    otel_attrs = {
        "dx.id": dx_id,
        "dx.channel": event.channel or "",
        "dx.operation": event.operation or "",
        "dx.remote_system": event.remote_system or "",
        "dx.address": event.address or "",
        "dx.state": event.state or "",
        "dx.attempt": event.attempt or 1,
        "dx.integration": event.integration or "",
    }

    # Add HTTP-specific attributes if present
    if event.http_method:
        otel_attrs["http.method"] = event.http_method
    if event.status_code:
        otel_attrs["http.status_code"] = event.status_code

    # Add timing info
    if event.occurred_at:
        otel_attrs["dx.occurred_at"] = event.occurred_at.isoformat()
    if event.completed_at:
        otel_attrs["dx.completed_at"] = event.completed_at.isoformat()
    if event.occurred_at and event.completed_at:
        duration_ms = int(
            (event.completed_at - event.occurred_at).total_seconds() * 1000
        )
        otel_attrs["dx.duration_ms"] = duration_ms

    # Add correlation info
    otel_attrs["fw.run_id"] = corr.run_id or ""
    if corr.span_id:
        otel_attrs["fw.span_id"] = corr.span_id

    # Add payload sizes if available
    if event.request_payload:
        otel_attrs["dx.request_size_bytes"] = len(event.request_payload)
    if event.response_payload:
        otel_attrs["dx.response_size_bytes"] = len(event.response_payload)

    # Add any extra attributes
    if event.attrs:
        for k, v in event.attrs.items():
            otel_attrs[f"dx.attr.{k}"] = str(v)

    # Add the event to the current span
    event_name = f"data_exchange.{event.channel or 'unknown'}"
    current_span.add_event(event_name, attributes=otel_attrs)

    # If the state indicates failure, set span status (but don't fail the span for HTTP errors)
    if event.state in ("FAILED", "TIMEOUT"):
        # We add an attribute but don't set the span to error since this is just one exchange
        current_span.set_attribute("dx.has_failed_exchange", True)


async def record_data_exchange(
    event: DataExchangeEvent, correlation: Optional[Correlation] = None
):
    corr = _get_correlation(correlation)
    dx_id = str(uuid.uuid4())

    # Emit OTEL span event so OTEL consumers see the same info
    # This runs synchronously and adds an event to the current span (if any)
    _emit_otel_data_exchange_event(event, dx_id, corr)

    # Logic to upload blobs and then write record
    # We offload the WHOLE process (upload + write) to the executor
    # to avoid blocking strict timeouts if upload is slow.

    def _process_dx():
        # --- Request payload ---
        req_ref = None
        req_payload = None
        req_size = None
        if event.request_payload:
            if event.offload_payloads:
                try:
                    req_ref, req_size, _ = get_blob_store().put(
                        path_hint=f"{corr.run_id}/dx/{dx_id}/request",
                        content_type=event.request_content_type
                        or "application/octet-stream",
                        data=event.request_payload,
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload request payload: {e}")
            else:
                req_payload = event.request_payload
                req_size = len(event.request_payload)

        # --- Response payload ---
        res_ref = None
        res_payload = None
        res_size = None
        if event.response_payload:
            if event.offload_payloads:
                try:
                    res_ref, res_size, _ = get_blob_store().put(
                        path_hint=f"{corr.run_id}/dx/{dx_id}/response",
                        content_type=event.response_content_type
                        or "application/octet-stream",
                        data=event.response_payload,
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload response payload: {e}")
            else:
                res_payload = event.response_payload
                res_size = len(event.response_payload)

        dx = DataExchange(
            id=dx_id,
            correlation=corr,
            integration=event.integration,
            channel=event.channel,
            operation=event.operation,
            remote_system=event.remote_system,
            address=event.address,
            occurred_at=event.occurred_at or datetime.now(UTC),
            completed_at=event.completed_at,
            state=event.state,
            attempt=event.attempt,
            retry_of_id=event.retry_of_id,
            duration_ms=(
                int((event.completed_at - event.occurred_at).total_seconds() * 1000)
                if event.completed_at and event.occurred_at
                else None
            ),
            http_method=event.http_method,
            status_code=event.status_code,
            request_payload_ref=req_ref,
            response_payload_ref=res_ref,
            request_content_type=event.request_content_type,
            response_content_type=event.response_content_type,
            request_size_bytes=req_size or event.request_size_bytes,
            response_size_bytes=res_size or event.response_size_bytes,
            request_payload=req_payload,
            response_payload=res_payload,
            attrs=event.attrs,
        )
        get_data_exchange_store().write_data_exchange(dx)

    await AsyncManager.get_instance().execute(_process_dx)

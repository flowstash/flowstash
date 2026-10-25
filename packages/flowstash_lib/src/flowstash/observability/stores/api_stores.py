import httpx
import logging
from typing import Tuple, Optional, Any
from dataclasses import is_dataclass, asdict
from datetime import datetime
import json
import base64
import threading
import time
import atexit
import os

logger = logging.getLogger(__name__)

from .protocols import EventsStore, DataExchangeStore, BlobStore, RecordsStore
from ..model import RunEvent, SpanEvent, Correlation, DataExchange, RecordLink


def _to_json_serializable(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if is_dataclass(obj):
        d = asdict(obj)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif isinstance(v, bytes):
                d[k] = base64.b64encode(v).decode("ascii")
            elif is_dataclass(v):
                d[k] = _to_json_serializable(v)
        return d
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_serializable(i) for i in obj]
    return obj


# API bulk-ingest limit per request (see managed/api/ingestion/router.py).
_BULK_MAX_ITEMS = 500

# Maps the per-event endpoint passed to submit() onto its bulk counterpart and the
# request field the API expects the list of events under (see BulkRunEventRequest /
# BulkSpanEventRequest / BulkLogRequest in managed/api/observability/models.py).
_BULK_ROUTES = {
    "/ingestion/events/run": ("/ingestion/events/run/bulk", "events"),
    "/ingestion/events/span": ("/ingestion/events/span/bulk", "events"),
    "/ingestion/logs": ("/ingestion/logs/bulk", "logs"),
    "/ingestion/data-exchanges": ("/ingestion/data-exchanges/bulk", "exchanges"),
    "/ingestion/records": ("/ingestion/records/bulk", "records"),
}
# Reverse map so a bulk POST that hits an API without that bulk route (404/405)
# can fall back to one POST per item instead of dropping the batch.
_SINGLE_ROUTES = {bulk: single for single, (bulk, _) in _BULK_ROUTES.items()}

# Approximate size of one buffered item when the caller gives no hint.
_DEFAULT_ITEM_BYTES = 1024

# Status codes worth retrying. 4xx (other than these) signal a permanent problem
# with the payload, so retrying only wastes time and pins a worker thread.
_TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


class _AsyncWorker:
    """Background worker that batches observability events per endpoint and ships
    them to the bulk ingest API with retry on transient errors.

    Events are accumulated in an in-memory buffer keyed by endpoint and flushed to
    the matching ``/bulk`` endpoint either when the buffer reaches ``BATCH_SIZE`` or
    every ``FLUSH_INTERVAL_MS`` by a background timer. Each batch POST retries with
    exponential backoff; batches containing a terminal (ENDED) run event retry far
    more persistently so run-lifecycle tracking is not left dangling.
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        if not api_key:
            raise ValueError("API key must be provided for ApiEventsStore")
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self._shutdown = False
        import concurrent.futures

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        # Terminal (run ENDED) batches get their own lane so they never queue
        # behind a backlog of data-exchange batches.
        self._terminal_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        # httpx.Client is thread-safe and shared across all executor threads.
        self._client = httpx.Client(headers=self.headers, timeout=30.0, transport=transport)

        # Event batching: endpoint -> list of (already JSON-serializable) payloads.
        self._event_buffer: dict[str, list] = {}
        self._buffer_bytes: dict[str, int] = {}
        self._buffer_lock = threading.Lock()

        # In-flight batch accounting so join() can wait for posted batches to land.
        self._inflight = 0
        self._inflight_terminal = 0
        self._inflight_cond = threading.Condition()

        # Endpoints whose bulk route the API does not serve (404/405): post singly.
        self._single_only: set[str] = set()

        # Tunable batching / retry configuration.
        self.BATCH_SIZE = min(
            int(os.getenv("FLOWSTASH_OBS_BATCH_SIZE", "100")), _BULK_MAX_ITEMS
        )
        # Flush a buffer early once its payloads add up to this many bytes, so
        # exchanges carrying inline bodies never build a multi-hundred-MB request.
        self.BATCH_MAX_BYTES = int(
            os.getenv("FLOWSTASH_OBS_BATCH_MAX_BYTES", str(4 * 1024 * 1024))
        )
        self.FLUSH_INTERVAL_MS = int(os.getenv("FLOWSTASH_OBS_FLUSH_INTERVAL_MS", "250"))
        self.RETRY_MAX_ATTEMPTS = int(os.getenv("FLOWSTASH_OBS_RETRY_MAX_ATTEMPTS", "5"))
        self.RETRY_TERMINAL_ATTEMPTS = int(
            os.getenv("FLOWSTASH_OBS_RETRY_TERMINAL_ATTEMPTS", "100")
        )
        self.RETRY_BACKOFF_BASE_MS = int(
            os.getenv("FLOWSTASH_OBS_RETRY_BACKOFF_BASE_MS", "100")
        )

        self._flush_thread = threading.Thread(target=self._flush_periodically, daemon=True)
        self._flush_thread.start()
        atexit.register(self.shutdown)

    # -- public API -----------------------------------------------------------

    def submit(self, endpoint: str, payload: Any, size_hint: Optional[int] = None):
        """Buffer an event; flush its endpoint once the item or byte threshold is hit."""
        if self._shutdown:
            return
        with self._buffer_lock:
            buf = self._event_buffer.setdefault(endpoint, [])
            buf.append(payload)
            self._buffer_bytes[endpoint] = self._buffer_bytes.get(endpoint, 0) + (
                size_hint if size_hint is not None else _DEFAULT_ITEM_BYTES
            )
            if len(buf) >= self.BATCH_SIZE or self._buffer_bytes[endpoint] >= self.BATCH_MAX_BYTES:
                self._flush_endpoint_locked(endpoint)

    def submit_terminal(self, endpoint: str, payload: Any):
        """Buffer a terminal event and ship its endpoint right away on the priority lane.

        Used for run ENDED: the event must not wait for the periodic timer nor sit
        behind queued data-exchange batches, otherwise a process that exits after a
        long backlog leaves the run dangling as RUNNING.
        """
        if self._shutdown:
            return
        with self._buffer_lock:
            self._event_buffer.setdefault(endpoint, []).append(payload)
            self._buffer_bytes[endpoint] = self._buffer_bytes.get(endpoint, 0) + _DEFAULT_ITEM_BYTES
            self._flush_endpoint_locked(endpoint, priority=True)

    def pending(self) -> dict:
        """Counts for drain progress logging: buffered items and in-flight batches."""
        with self._buffer_lock:
            buffered = sum(len(b) for b in self._event_buffer.values())
        with self._inflight_cond:
            return {
                "buffered": buffered,
                "inflight_batches": self._inflight,
                "inflight_terminal": self._inflight_terminal,
            }

    def join_terminal(self, timeout: float = 10.0) -> bool:
        """Block until every in-flight terminal batch has landed. True if drained."""
        deadline = time.monotonic() + timeout
        with self._inflight_cond:
            while self._inflight_terminal > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._inflight_cond.wait(timeout=remaining)
        return True

    def join(self, timeout: float = 10.0) -> None:
        """Flush buffered events, then block until in-flight batches land or timeout."""
        self._flush_all()
        deadline = time.monotonic() + timeout
        with self._inflight_cond:
            while self._inflight > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._inflight_cond.wait(timeout=remaining)
            pending = self._inflight
        if pending > 0:
            logger.warning(
                "Observability flush timed out after %.1fs with %d batch(es) still in flight; "
                "some events may not have been delivered.",
                timeout,
                pending,
            )

    def shutdown(self):
        self._shutdown = True
        self.join(timeout=5.0)
        for executor in (self._executor, self._terminal_executor):
            try:
                executor.shutdown(wait=False)
            except Exception:
                pass
        try:
            self._client.close()
        except Exception:
            pass

    # -- flushing -------------------------------------------------------------

    def _flush_periodically(self):
        """Background timer: drain all endpoint buffers on a fixed interval."""
        interval = self.FLUSH_INTERVAL_MS / 1000.0
        while not self._shutdown:
            time.sleep(interval)
            try:
                self._flush_all()
            except Exception:
                logger.exception("Observability: periodic flush failed")

    def _flush_all(self):
        with self._buffer_lock:
            for endpoint in list(self._event_buffer.keys()):
                if self._event_buffer.get(endpoint):
                    self._flush_endpoint_locked(endpoint)

    def _flush_endpoint_locked(self, endpoint: str, priority: bool = False):
        """Drain one endpoint's buffer and dispatch it to the executor.

        Caller must hold ``self._buffer_lock``. Batches larger than the API limit are
        split so a single oversized periodic flush still succeeds.
        """
        batch = self._event_buffer.pop(endpoint, None)
        self._buffer_bytes.pop(endpoint, None)
        if not batch:
            return
        bulk_endpoint, field = _BULK_ROUTES.get(endpoint, (endpoint + "/bulk", "events"))
        for start in range(0, len(batch), _BULK_MAX_ITEMS):
            chunk = batch[start : start + _BULK_MAX_ITEMS]
            is_terminal = priority or (field == "events" and self._chunk_has_terminal(chunk))
            with self._inflight_cond:
                self._inflight += 1
                if is_terminal:
                    self._inflight_terminal += 1
            executor = self._terminal_executor if is_terminal else self._executor
            try:
                executor.submit(self._post_batch, bulk_endpoint, field, chunk, is_terminal)
            except RuntimeError:
                # Interpreter (or this worker) is shutting down and the pool refuses
                # new work. The chunk is already off the buffer, so post it here
                # rather than lose it. This is the path a run ENDED takes when the
                # process exits with a backlog.
                logger.warning(
                    "Observability: executor unavailable, posting %d item(s) to %s inline",
                    len(chunk),
                    bulk_endpoint,
                )
                self._post_batch(bulk_endpoint, field, chunk, is_terminal)

    @staticmethod
    def _chunk_has_terminal(chunk: list) -> bool:
        """True if any payload in the chunk is a terminal (ENDED) run event."""
        return any(
            isinstance(e, dict) and e.get("event_type") == "ENDED" for e in chunk
        )

    # -- delivery -------------------------------------------------------------

    def _post_batch(self, endpoint: str, field: str, batch: list, is_terminal: bool):
        max_attempts = (
            self.RETRY_TERMINAL_ATTEMPTS if is_terminal else self.RETRY_MAX_ATTEMPTS
        )
        payload = {field: batch}
        url = f"{self.api_url}{endpoint}"
        single_endpoint = _SINGLE_ROUTES.get(endpoint)
        try:
            if single_endpoint and endpoint in self._single_only:
                self._post_singly(single_endpoint, batch, max_attempts)
                return
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = self._client.post(url, json=payload)
                    resp.raise_for_status()
                    return
                except Exception as e:  # @IgnoreException
                    if (
                        single_endpoint
                        and isinstance(e, httpx.HTTPStatusError)
                        and e.response.status_code in (404, 405)
                    ):
                        # Older API without this bulk route: degrade to per-item
                        # posts for the rest of this process instead of dropping.
                        if endpoint not in self._single_only:
                            self._single_only.add(endpoint)
                            logger.warning(
                                "Observability: %s not served by the API (%s); "
                                "falling back to single posts on %s",
                                endpoint, e.response.status_code, single_endpoint,
                            )
                        self._post_singly(single_endpoint, batch, max_attempts)
                        return
                    transient = self._is_transient(e)
                    if not transient or attempt >= max_attempts:
                        level = logger.error if transient else logger.warning
                        level(
                            "Observability: dropping batch of %d for %s after %d attempt(s) — %s. "
                            "Check that managed_api_url is correct and the managed API is running.",
                            len(batch),
                            endpoint,
                            attempt,
                            e,
                        )
                        return
                    backoff_ms = min(
                        self.RETRY_BACKOFF_BASE_MS * (2 ** (attempt - 1)), 10000
                    )
                    logger.warning(
                        "Observability: POST %s failed (attempt %d/%d): %s. Retrying in %dms.",
                        endpoint,
                        attempt,
                        max_attempts,
                        e,
                        backoff_ms,
                    )
                    time.sleep(backoff_ms / 1000.0)
        finally:
            with self._inflight_cond:
                self._inflight -= 1
                if is_terminal:
                    self._inflight_terminal -= 1
                self._inflight_cond.notify_all()

    def _post_singly(self, endpoint: str, batch: list, max_attempts: int) -> None:
        url = f"{self.api_url}{endpoint}"
        for item in batch:
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = self._client.post(url, json=item)
                    resp.raise_for_status()
                    break
                except Exception as e:  # @IgnoreException
                    if not self._is_transient(e) or attempt >= max_attempts:
                        logger.warning(
                            "Observability: dropping 1 item for %s after %d attempt(s) — %s",
                            endpoint, attempt, e,
                        )
                        break
                    time.sleep(min(self.RETRY_BACKOFF_BASE_MS * (2 ** (attempt - 1)), 10000) / 1000.0)

    @staticmethod
    def _is_transient(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in _TRANSIENT_STATUS
        return isinstance(exc, (httpx.TransportError, TimeoutError, OSError))


def _enrich_correlation(
    payload: dict, project_id: Optional[str], environment: Optional[str]
) -> None:
    """Inject project_id and environment into the nested correlation dict of a payload."""
    correlation = payload.get("correlation")
    if isinstance(correlation, dict):
        if project_id is not None:
            correlation["project_id"] = project_id
        if environment is not None:
            correlation["environment"] = environment


class ApiEventsStore(EventsStore):
    def __init__(
        self,
        api_url: str,
        api_key: Optional[str] = None,
        *,
        project_id: Optional[str] = None,
        environment: Optional[str] = None,
        worker: Optional[_AsyncWorker] = None,
    ):
        self.worker = worker or _AsyncWorker(api_url, api_key)
        self._project_id = project_id
        self._environment = environment

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all queued events have been delivered (or timeout expires)."""
        self.worker.join(timeout=timeout)

    def flush_terminal(self, timeout: float = 10.0) -> bool:
        """Block only until in-flight run ENDED batches have landed."""
        return self.worker.join_terminal(timeout=timeout)

    def pending(self) -> dict:
        return self.worker.pending()

    def write_run_event(self, event: RunEvent) -> None:
        payload = _to_json_serializable(event)
        _enrich_correlation(payload, self._project_id, self._environment)
        if event.event_type == "ENDED":
            self.worker.submit_terminal("/ingestion/events/run", payload)
        else:
            self.worker.submit("/ingestion/events/run", payload)

    def write_span_event(self, event: SpanEvent) -> None:
        payload = _to_json_serializable(event)
        _enrich_correlation(payload, self._project_id, self._environment)
        self.worker.submit("/ingestion/events/span", payload)

    def write_log(
        self,
        correlation: Correlation,
        severity: str,
        message: str,
        attrs: dict | None = None,
    ) -> None:
        payload = {
            "correlation": _to_json_serializable(correlation),
            "severity": severity,
            "message": message,
            "occurred_at": datetime.utcnow().isoformat(),
            "attrs": attrs or {},
        }
        _enrich_correlation(payload, self._project_id, self._environment)
        self.worker.submit("/ingestion/logs", payload)


class ApiDataExchangeStore(DataExchangeStore):
    def __init__(
        self,
        api_url: str,
        api_key: Optional[str] = None,
        *,
        project_id: Optional[str] = None,
        environment: Optional[str] = None,
        worker: Optional[_AsyncWorker] = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self._project_id = project_id
        self._environment = environment
        self.worker = worker

    def write_data_exchange(self, dx: DataExchange) -> None:
        payload = _to_json_serializable(dx)
        _enrich_correlation(payload, self._project_id, self._environment)
        if self.worker is not None:
            size_hint = _DEFAULT_ITEM_BYTES + sum(
                len(v) for v in (dx.request_payload, dx.response_payload) if v
            )
            self.worker.submit("/ingestion/data-exchanges", payload, size_hint=size_hint)
            return
        try:
            resp = self.client.post(
                f"{self.api_url}/ingestion/data-exchanges",
                json=payload,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(
                "Observability: failed to ship data exchange to %s/ingestion/data-exchanges — %s. Payload: %s",
                self.api_url,
                e,
                payload,
            )


class ApiRecordsStore(RecordsStore):
    def __init__(
        self,
        api_url: str,
        api_key: Optional[str] = None,
        *,
        project_id: Optional[str] = None,
        environment: Optional[str] = None,
        worker: Optional[_AsyncWorker] = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=10.0)
        self._project_id = project_id
        self._environment = environment
        self.worker = worker

    def write_record_link(self, link: RecordLink) -> None:
        payload = _to_json_serializable(link)
        if self._project_id is not None:
            payload["project_id"] = self._project_id
        if self._environment is not None:
            payload["environment"] = self._environment
        if self.worker is not None:
            self.worker.submit("/ingestion/records", payload)
            return
        try:
            resp = self.client.post(
                f"{self.api_url}/ingestion/records",
                json=payload,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(
                "Observability: failed to ship record link to %s/ingestion/records — %s. Payload: %s",
                self.api_url,
                e,
                payload,
            )


class ApiBlobStore(BlobStore):
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    def put(
        self, *, path_hint: str, content_type: str, data: bytes
    ) -> Tuple[str, int, str]:
        try:
            files = {"file": (path_hint, data, content_type)}
            resp = self.client.post(f"{self.api_url}/ingestion/blobs", files=files)
            resp.raise_for_status()
            res = resp.json()
            return res["payload_ref"], res["size_bytes"], res["sha256"]
        except Exception as e:
            logger.warning(
                "Observability: failed to upload blob %s to %s/ingestion/blobs — %s",
                path_hint,
                self.api_url,
                e,
            )
            return f"error://{path_hint}", len(data), "error-sha"

    def get(self, payload_ref: str) -> bytes:
        try:
            resp = self.client.get(
                f"{self.api_url}/observability/blobs", params={"ref": payload_ref}
            )
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(
                "Observability: failed to retrieve blob %s from %s/observability/blobs — %s",
                payload_ref,
                self.api_url,
                e,
            )
            return b""

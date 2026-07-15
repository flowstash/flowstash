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
}

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

    def __init__(self, api_url: str, api_key: str):
        if not api_key:
            raise ValueError("API key must be provided for ApiEventsStore")
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self._shutdown = False
        import concurrent.futures

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        # httpx.Client is thread-safe and shared across all executor threads.
        self._client = httpx.Client(headers=self.headers, timeout=30.0)

        # Event batching: endpoint -> list of (already JSON-serializable) payloads.
        self._event_buffer: dict[str, list] = {}
        self._buffer_lock = threading.Lock()

        # In-flight batch accounting so join() can wait for posted batches to land.
        self._inflight = 0
        self._inflight_cond = threading.Condition()

        # Tunable batching / retry configuration.
        self.BATCH_SIZE = min(
            int(os.getenv("FLOWSTASH_OBS_BATCH_SIZE", "100")), _BULK_MAX_ITEMS
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

    def submit(self, endpoint: str, payload: Any):
        """Buffer an event; flush its endpoint once the batch-size threshold is hit."""
        if self._shutdown:
            return
        with self._buffer_lock:
            buf = self._event_buffer.setdefault(endpoint, [])
            buf.append(payload)
            if len(buf) >= self.BATCH_SIZE:
                self._flush_endpoint_locked(endpoint)

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
        try:
            self._executor.shutdown(wait=False)
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

    def _flush_endpoint_locked(self, endpoint: str):
        """Drain one endpoint's buffer and dispatch it to the executor.

        Caller must hold ``self._buffer_lock``. Batches larger than the API limit are
        split so a single oversized periodic flush still succeeds.
        """
        batch = self._event_buffer.pop(endpoint, None)
        if not batch:
            return
        bulk_endpoint, field = _BULK_ROUTES.get(endpoint, (endpoint + "/bulk", "events"))
        for start in range(0, len(batch), _BULK_MAX_ITEMS):
            chunk = batch[start : start + _BULK_MAX_ITEMS]
            is_terminal = field == "events" and self._chunk_has_terminal(chunk)
            with self._inflight_cond:
                self._inflight += 1
            self._executor.submit(
                self._post_batch, bulk_endpoint, field, chunk, is_terminal
            )

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
        try:
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = self._client.post(url, json=payload)
                    resp.raise_for_status()
                    return
                except Exception as e:  # @IgnoreException
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
                self._inflight_cond.notify_all()

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
    ):
        self.worker = _AsyncWorker(api_url, api_key)
        self._project_id = project_id
        self._environment = environment

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all queued events have been delivered (or timeout expires)."""
        self.worker.join(timeout=timeout)

    def write_run_event(self, event: RunEvent) -> None:
        payload = _to_json_serializable(event)
        _enrich_correlation(payload, self._project_id, self._environment)
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
    ):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self._project_id = project_id
        self._environment = environment

    def write_data_exchange(self, dx: DataExchange) -> None:
        payload = _to_json_serializable(dx)
        _enrich_correlation(payload, self._project_id, self._environment)
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
    ):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=10.0)
        self._project_id = project_id
        self._environment = environment

    def write_record_link(self, link: RecordLink) -> None:
        payload = _to_json_serializable(link)
        if self._project_id is not None:
            payload["project_id"] = self._project_id
        if self._environment is not None:
            payload["environment"] = self._environment
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

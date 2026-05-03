import httpx
import logging
from typing import Tuple, Optional, Any
from dataclasses import is_dataclass, asdict
from datetime import datetime
import json
import threading
import queue
import time
import atexit

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
            if is_dataclass(v):
                d[k] = _to_json_serializable(v)
        return d
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_serializable(i) for i in obj]
    return obj


class _AsyncWorker:
    """A background worker that processes observation events in batches or one by one."""

    def __init__(self, api_url: str, api_key: str):
        if not api_key:
            raise ValueError("API key must be provided for ApiEventsStore")
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self._queue = queue.Queue(maxsize=10000)
        self._shutdown = False
        self._worker_thread = threading.Thread(target=self._run, daemon=True)
        self._worker_thread.start()
        atexit.register(self.shutdown)

    def _run(self):
        with httpx.Client(headers=self.headers, timeout=10.0) as client:
            while not self._shutdown or not self._queue.empty():
                try:
                    task = self._queue.get(timeout=0.1)  # @IgnoreException
                except queue.Empty:
                    continue

                endpoint, payload = task
                try:
                    resp = client.post(f"{self.api_url}{endpoint}", json=payload)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(
                        "Observability: failed to POST to %s%s — %s. Payload: %s. "
                        "Check that managed_api_url is correct and the managed API is running.",
                        self.api_url,
                        endpoint,
                        e,
                        payload,
                    )
                finally:
                    self._queue.task_done()

    def submit(self, endpoint: str, payload: Any):
        if self._shutdown:
            return
        try:
            self._queue.put_nowait((endpoint, payload))
        except queue.Full:
            logger.warning("Observability queue full, dropping event for %s", endpoint)

    def join(self, timeout: float = 10.0) -> None:
        """Block until all queued items have been processed or timeout expires."""
        t = threading.Thread(target=self._queue.join, daemon=True)
        t.start()
        t.join(timeout=timeout)

    def shutdown(self):
        self._shutdown = True
        self.join(timeout=5.0)


class ApiEventsStore(EventsStore):
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        self.worker = _AsyncWorker(api_url, api_key)

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all queued events have been delivered (or timeout expires)."""
        self.worker.join(timeout=timeout)

    def write_run_event(self, event: RunEvent) -> None:
        self.worker.submit("/ingestion/events/run", _to_json_serializable(event))

    def write_span_event(self, event: SpanEvent) -> None:
        self.worker.submit("/ingestion/events/span", _to_json_serializable(event))

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
        self.worker.submit("/ingestion/logs", payload)


class ApiDataExchangeStore(DataExchangeStore):
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        # reuse the logic if we want, but for now just use simple client or a shared worker
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=10.0)

    def write_data_exchange(self, dx: DataExchange) -> None:
        payload = _to_json_serializable(dx)
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
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        self.api_url = api_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.Client(headers=self.headers, timeout=10.0)

    def write_record_link(self, link: RecordLink) -> None:
        payload = _to_json_serializable(link)
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

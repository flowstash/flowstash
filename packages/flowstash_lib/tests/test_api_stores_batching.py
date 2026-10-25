"""Batching worker behaviour: bulk routes for every event kind, terminal priority,
shutdown safety, and the fallback for APIs without a bulk route."""

import json
import threading
from datetime import datetime, UTC

import httpx
import pytest

from flowstash.observability.model import (
    Correlation,
    DataExchange,
    RecordLink,
    RecordLinkKind,
    RunEvent,
)
from flowstash.observability.stores.api_stores import (
    ApiDataExchangeStore,
    ApiEventsStore,
    ApiRecordsStore,
    _AsyncWorker,
)
from flowstash.state.stores.managed_store import ManagedStateStore


class _Capture:
    """Records every request; `status_for` decides the response per path."""

    def __init__(self, status_for=None):
        self.requests: list[httpx.Request] = []
        self._lock = threading.Lock()
        self._status_for = status_for or (lambda path: 200)

    def handler(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.requests.append(request)
        return httpx.Response(self._status_for(request.url.path), json={"ok": True})

    def paths(self):
        with self._lock:
            return [r.url.path for r in self.requests]

    def bodies(self, path):
        with self._lock:
            return [json.loads(r.content) for r in self.requests if r.url.path == path]


def _worker(capture: _Capture, **env) -> _AsyncWorker:
    w = _AsyncWorker("https://api.test", "key", transport=httpx.MockTransport(capture.handler))
    w.FLUSH_INTERVAL_MS = 10_000  # periodic timer must not interfere with tests
    for k, v in env.items():
        setattr(w, k, v)
    return w


def _dx(i: int, payload: bytes = b"") -> DataExchange:
    now = datetime.now(UTC)
    return DataExchange(
        id=f"dx-{i}", correlation=Correlation(run_id="run-1"), integration="shop",
        channel="HTTP", operation="GET", remote_system=None, address="https://x",
        occurred_at=now, completed_at=now, state="SUCCESS",
        request_payload=payload or None,
    )


def test_data_exchanges_are_batched_to_bulk_route():
    cap = _Capture()
    w = _worker(cap)
    store = ApiDataExchangeStore("https://api.test", "key", project_id="p", environment="prod", worker=w)
    for i in range(3):
        store.write_data_exchange(_dx(i))
    w.join(timeout=5)

    assert cap.paths() == ["/ingestion/data-exchanges/bulk"]
    body = cap.bodies("/ingestion/data-exchanges/bulk")[0]
    assert [e["id"] for e in body["exchanges"]] == ["dx-0", "dx-1", "dx-2"]
    assert body["exchanges"][0]["correlation"]["project_id"] == "p"


def test_record_links_are_batched_to_bulk_route():
    cap = _Capture()
    w = _worker(cap)
    store = ApiRecordsStore("https://api.test", "key", project_id="p", environment="prod", worker=w)
    for i in range(2):
        store.write_record_link(RecordLink(tenant_id="t", run_id="run-1", record_key=f"r-{i}", kind=RecordLinkKind.PUBLISHED))
    w.join(timeout=5)

    assert cap.paths() == ["/ingestion/records/bulk"]
    body = cap.bodies("/ingestion/records/bulk")[0]
    assert [r["record_key"] for r in body["records"]] == ["r-0", "r-1"]
    assert body["records"][0]["project_id"] == "p"


def test_many_record_links_share_few_bulk_posts():
    cap = _Capture()
    w = _worker(cap)
    store = ApiRecordsStore("https://api.test", "key", project_id="p", worker=w)
    assert store.buffered is True
    store.write_record_links([
        RecordLink(tenant_id="t", run_id="run-1", record_key=f"r-{i}", kind=RecordLinkKind.LINKED)
        for i in range(250)
    ])
    store.flush(timeout=5)

    bodies = cap.bodies("/ingestion/records/bulk")
    assert len(bodies) == 3
    assert sum(len(b["records"]) for b in bodies) == 250


def test_bulk_route_missing_falls_back_to_single_posts():
    cap = _Capture(status_for=lambda path: 404 if path.endswith("/bulk") else 200)
    w = _worker(cap)
    store = ApiDataExchangeStore("https://api.test", "key", worker=w)
    for i in range(2):
        store.write_data_exchange(_dx(i))
    w.join(timeout=5)
    assert cap.paths() == ["/ingestion/data-exchanges/bulk", "/ingestion/data-exchanges", "/ingestion/data-exchanges"]

    # Once the API said 404, later batches skip the bulk route entirely.
    store.write_data_exchange(_dx(9))
    w.join(timeout=5)
    assert cap.paths()[-1] == "/ingestion/data-exchanges"
    assert cap.paths().count("/ingestion/data-exchanges/bulk") == 1


def test_byte_cap_flushes_before_item_cap():
    cap = _Capture()
    w = _worker(cap, BATCH_SIZE=100, BATCH_MAX_BYTES=10_000)
    store = ApiDataExchangeStore("https://api.test", "key", worker=w)
    store.write_data_exchange(_dx(0, payload=b"x" * 6_000))
    assert cap.paths() == []  # under the cap, still buffered
    store.write_data_exchange(_dx(1, payload=b"x" * 6_000))
    w.join(timeout=5)
    assert cap.paths() == ["/ingestion/data-exchanges/bulk"]
    assert len(cap.bodies("/ingestion/data-exchanges/bulk")[0]["exchanges"]) == 2


def test_run_ended_ships_on_priority_lane_without_waiting_for_timer():
    cap = _Capture()
    w = _worker(cap)
    store = ApiEventsStore("https://api.test", "key", worker=w)
    store.write_run_event(RunEvent(event_type="STARTED", correlation=Correlation(run_id="run-1")))
    assert cap.paths() == []  # non-terminal events wait for the batch/timer
    store.write_run_event(RunEvent(event_type="ENDED", status="FAILED", correlation=Correlation(run_id="run-1")))
    assert store.flush_terminal(timeout=5) is True
    assert "/ingestion/events/run/bulk" in cap.paths()
    sent = cap.bodies("/ingestion/events/run/bulk")[0]["events"]
    assert any(e["event_type"] == "ENDED" and e["status"] == "FAILED" for e in sent)
    assert w.pending()["inflight_terminal"] == 0


def test_flush_after_executor_shutdown_posts_inline_instead_of_dropping():
    cap = _Capture()
    w = _worker(cap)
    w._executor.shutdown(wait=True)  # mimics interpreter shutdown refusing new futures
    w.submit("/ingestion/logs", {"message": "late"})
    w._flush_all()
    assert cap.paths() == ["/ingestion/logs/bulk"]
    assert w.pending()["inflight_batches"] == 0


def test_managed_state_store_4xx_surfaces_api_detail_without_retry():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(400, json={"detail": "State value too large: limit is 1048576 bytes"})

    store = ManagedStateStore(
        base_url="https://api.test", api_key="k",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(ValueError) as exc:
        store.set("ns", "key", {"a": 1})
    assert "State value too large" in str(exc.value)
    assert "400" in str(exc.value)
    assert len(calls) == 1

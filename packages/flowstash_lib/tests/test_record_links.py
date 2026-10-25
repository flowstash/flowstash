"""record_scope / link_record / link_records: keys, context handling, and
propagation of the current record to submitted tasks."""

import asyncio
import threading

import pytest

from flowstash.context import current_context, integration_context
from flowstash.decorators import integration_task
from flowstash.observability.model import RecordLinkKind
from flowstash.observability.registry import get_records_store, set_records_store
from flowstash.propagation import (
    MAX_VALUE_BYTES,
    PROPAGATED_RECORD_KEY,
    context_kwargs_from_propagation,
    extract_propagation,
)
from flowstash.queue.asyncio_backend import AsyncioBackend
from flowstash.queue.backend import set_backend
from flowstash.records import link_record, link_records, record_scope


class _CaptureRecords:
    """Buffered store: links are written inline, so tests see them at once."""

    buffered = True

    def __init__(self):
        self.links = []
        self._lock = threading.Lock()

    def write_record_link(self, link):
        with self._lock:
            self.links.append(link)

    def write_record_links(self, links):
        with self._lock:
            self.links.extend(links)

    def keys(self):
        return [link.record_key for link in self.links]


@pytest.fixture
def store(monkeypatch):
    previous = get_records_store()
    capture = _CaptureRecords()
    set_records_store(capture)
    yield capture
    set_records_store(previous)


def _run(**kwargs):
    return integration_context(
        integration="shop", integration_pipeline="sync", record_lifecycle=False, **kwargs
    )


# -- link_record / link_records ------------------------------------------------


def test_outside_run_is_noop(store):
    link_record("1", record_type="order")
    assert link_records(["1", "2"], record_type="order") == 0
    with record_scope("1", record_type="order") as key:
        assert key is None
    assert store.links == []


def test_link_record_builds_linked_link(store):
    with _run() as ctx:
        link_record(42, record_type="order")

    [link] = store.links
    assert link.record_key == "shop:order:42"
    assert link.kind == RecordLinkKind.LINKED
    assert link.run_id == ctx.run_id
    assert link.span_id == ctx.span_id
    assert link.pipeline == "sync"


def test_key_override_and_blank_ids(store):
    with _run():
        link_record("x", record_type="order", key="customer:7")
        link_record("", record_type="order")
        link_record(None, record_type="order")
        assert link_records(["", None, "3"], record_type="order") == 1
    assert store.keys() == ["customer:7", "shop:order:3"]


def test_link_records_reads_generator_without_limit(store):
    with _run():
        emitted = link_records((str(i) for i in range(12_000)), record_type="upc")

    assert emitted == 12_000
    assert len(store.links) == 12_000


# -- record_scope ------------------------------------------------------------------


def test_record_scope_sets_and_restores_current_record(store):
    with _run() as ctx:
        with record_scope("7", record_type="order") as key:
            inner = current_context()
            assert key == "shop:order:7"
            assert inner.current_record_key == key
            assert inner.span_id == ctx.span_id  # no extra span by default
        assert current_context().current_record_key is None
    assert store.keys() == ["shop:order:7"]


def test_record_scope_span_option_opens_span(store):
    with _run() as ctx:
        with record_scope("7", record_type="order", span=True):
            inner = current_context()
            assert inner.span_id != ctx.span_id
            assert inner.current_record_key == "shop:order:7"
    [link] = store.links
    assert link.span_id != ctx.span_id


@pytest.mark.asyncio
async def test_record_scope_is_isolated_per_task(store):
    seen = {}

    async def work(upc):
        with record_scope(upc, record_type="upc"):
            await asyncio.sleep(0)
            seen[upc] = current_context().current_record_key

    with _run():
        await asyncio.gather(*(work(str(i)) for i in range(20)))

    assert seen == {str(i): f"shop:upc:{i}" for i in range(20)}
    assert len(store.links) == 20


def test_integration_context_accepts_current_record_key():
    # Regression: the argument used to be dropped, so @consumer never set it.
    with _run(current_record_key="shop:order:1"):
        assert current_context().current_record_key == "shop:order:1"


# -- propagation -------------------------------------------------------------------


def test_extract_propagation(store):
    assert extract_propagation(None) == {}
    with _run():
        assert extract_propagation(current_context()) == {}
        with record_scope("1", record_type="order"):
            assert extract_propagation(current_context()) == {
                PROPAGATED_RECORD_KEY: "shop:order:1"
            }
        with record_scope("1", record_type="order", propagate=False):
            assert extract_propagation(current_context()) == {}
        with record_scope("x", record_type="order", key="k" * (MAX_VALUE_BYTES + 1)):
            assert extract_propagation(current_context()) == {}


def test_context_kwargs_from_propagation():
    assert context_kwargs_from_propagation(None) == {}
    assert context_kwargs_from_propagation({"other": "x"}) == {}
    assert context_kwargs_from_propagation({PROPAGATED_RECORD_KEY: "a:b:c"}) == {
        "current_record_key": "a:b:c"
    }


@pytest.fixture
def asyncio_backend():
    from flowstash.queue import backend as backend_module

    previous = backend_module._backend
    backend = AsyncioBackend()
    set_backend(backend)
    yield backend
    backend_module._backend = previous


@integration_task(integration="shop", integration_pipeline="fulfil")
async def _child_task(results: dict):
    ctx = current_context()
    results["record_key"] = ctx.current_record_key
    results["run_id"] = ctx.run_id


@pytest.mark.asyncio
async def test_submitted_task_inherits_record(store, asyncio_backend):
    results: dict = {}
    with _run() as parent:
        with record_scope("9", record_type="order"):
            _child_task.submit(results)
        await asyncio_backend.drain()

    assert results["record_key"] == "shop:order:9"
    child_links = [link for link in store.links if link.run_id == results["run_id"]]
    assert [(link.record_key, link.source) for link in child_links] == [
        ("shop:order:9", f"run:{parent.run_id}")
    ]


@pytest.mark.asyncio
async def test_propagate_false_keeps_child_clean(store, asyncio_backend):
    results: dict = {}
    with _run():
        with record_scope("9", record_type="order", propagate=False):
            _child_task.submit(results)
        await asyncio_backend.drain()

    assert results["record_key"] is None
    assert all(link.run_id != results["run_id"] for link in store.links)


@pytest.mark.asyncio
async def test_link_records_does_not_propagate(store, asyncio_backend):
    results: dict = {}
    with _run():
        link_records(["1", "2"], record_type="order")
        _child_task.submit(results)
        await asyncio_backend.drain()

    assert results["record_key"] is None


# -- transport ---------------------------------------------------------------------


def test_unbuffered_store_writes_off_the_caller_thread(store):
    from flowstash.observability.ingestion import AsyncManager

    store.buffered = False
    writer_threads = set()
    original = store.write_record_links

    def write(links):
        writer_threads.add(threading.get_ident())
        original(links)

    store.write_record_links = write
    with _run():
        link_records(["1", "2"], record_type="order")
    AsyncManager.get_instance().flush(timeout=5)

    assert store.keys() == ["shop:order:1", "shop:order:2"]
    assert threading.get_ident() not in writer_threads


def test_managed_submit_payload_keeps_propagated():
    from flowstash.observability.model import TaskDelegationMetadata
    from flowstash.queue.backends.managed_tasks import ManagedTasksBackend

    data = ManagedTasksBackend._serialize_delegation(
        TaskDelegationMetadata(
            parent_run_id="p",
            operation_id="op",
            target_task="m.t",
            propagated={PROPAGATED_RECORD_KEY: "shop:order:9"},
        )
    )
    assert data["propagated"] == {PROPAGATED_RECORD_KEY: "shop:order:9"}

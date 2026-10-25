"""The current record follows delegated work into the managed and dramatiq workers."""

import threading
from types import SimpleNamespace

import pytest

import flowstash.runtime.worker.backends.managed.http_entrypoint as http_entrypoint
from flowstash.context import current_context, integration_context
from flowstash.observability import record_links as rl
from flowstash.observability.model import RecordLinkKind, TaskDelegationMetadata
from flowstash.observability.registry import get_records_store, set_records_store
from flowstash.propagation import PROPAGATED_RECORD_KEY
from flowstash.runtime.worker.backends.dramatiq.dramatiq_backend import (
    DramatiqBackend,
    FrameworkContextMiddleware,
)


class _CaptureRecords:
    buffered = True

    def __init__(self):
        self.links = []
        self._lock = threading.Lock()

    def write_record_link(self, link):
        self.write_record_links([link])

    def write_record_links(self, links):
        with self._lock:
            self.links.extend(links)


@pytest.fixture
def store():
    previous = get_records_store()
    capture = _CaptureRecords()
    set_records_store(capture)
    rl._budgets.clear()
    yield capture
    set_records_store(previous)
    rl._budgets.clear()


@pytest.mark.asyncio
async def test_managed_worker_restores_propagated_record(monkeypatch, store):
    async def noop(*args, **kwargs):
        return None

    seen = {}

    async def task():
        ctx = current_context()
        seen["record_key"] = ctx.current_record_key
        seen["run_id"] = ctx.run_id

    monkeypatch.setattr(http_entrypoint, "record_run_started", noop)
    monkeypatch.setattr(http_entrypoint, "record_run_ended", noop)
    monkeypatch.setattr(http_entrypoint, "get_lease_client", lambda: None)
    monkeypatch.setattr(http_entrypoint, "_resolve_task_callable", lambda *_: task)

    payload = http_entrypoint.TaskPayload(
        task_name="t",
        func_ref="m.t",
        integration="shop",
        pipeline="orders",
        run_id="child-run",
        delegation={
            "parent_run_id": "parent-run",
            "operation_id": "op-1",
            "propagated": {PROPAGATED_RECORD_KEY: "shop:order:9"},
        },
    )
    await http_entrypoint._execute_managed_task(payload)

    assert seen == {"record_key": "shop:order:9", "run_id": "child-run"}
    [link] = store.links
    assert link.run_id == "child-run"
    assert link.record_key == "shop:order:9"
    assert link.kind == RecordLinkKind.LINKED
    assert link.source == "run:parent-run"


def test_managed_worker_accepts_payload_without_propagated():
    model = http_entrypoint.DelegationMetadataModel(parent_run_id="p")
    assert model.propagated == {}


def test_dramatiq_headers_carry_propagated_values():
    delegation = TaskDelegationMetadata(
        parent_run_id="p",
        operation_id="op",
        target_task="m.t",
        propagated={PROPAGATED_RECORD_KEY: "shop:order:9"},
    )
    with integration_context(integration="shop", integration_pipeline="orders") as ctx:
        headers = DramatiqBackend()._prepare_headers(ctx, tags=None, delegation=delegation)
        without = DramatiqBackend()._prepare_headers(
            ctx,
            tags=None,
            delegation=TaskDelegationMetadata(
                parent_run_id="p", operation_id="op", target_task="m.t"
            ),
        )

    assert headers["fw.propagated"] == {PROPAGATED_RECORD_KEY: "shop:order:9"}
    assert "fw.propagated" not in without


def test_dramatiq_middleware_restores_propagated_record(monkeypatch, store):
    import flowstash.runtime.worker.backends.dramatiq.dramatiq_backend as dramatiq_backend

    monkeypatch.setattr(
        dramatiq_backend, "_get_async_runner", lambda: SimpleNamespace(fire=lambda coro: coro.close())
    )
    message = SimpleNamespace(
        options={
            "headers": {
                "fw.integration": "shop",
                "fw.pipeline": "orders",
                "fw.parent_run_id": "parent-run",
                "fw.propagated": {PROPAGATED_RECORD_KEY: "shop:order:9"},
            }
        },
        args=(),
        kwargs={},
        actor=SimpleNamespace(fn=lambda: None),
        actor_name="t",
    )
    middleware = FrameworkContextMiddleware()
    middleware.before_process_message(None, message)
    try:
        assert current_context().current_record_key == "shop:order:9"
        run_id = current_context().run_id
    finally:
        middleware.after_process_message(None, message)

    [link] = store.links
    assert (link.run_id, link.record_key, link.source) == (
        run_id,
        "shop:order:9",
        "run:parent-run",
    )

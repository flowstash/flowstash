import asyncio
from contextlib import contextmanager

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

import flowstash.runtime.worker.backends.managed.http_entrypoint as http_entrypoint
from flowstash.runtime.worker.backends.managed.drain import ManagedTaskDrainController


@contextmanager
def _fake_integration_context(**kwargs):
    class Ctx:
        corelation = object()

    yield Ctx()


@pytest.fixture(autouse=True)
def patch_observability(monkeypatch):
    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(
        http_entrypoint, "integration_context", _fake_integration_context
    )
    monkeypatch.setattr(http_entrypoint, "record_run_started", noop)
    monkeypatch.setattr(http_entrypoint, "record_run_ended", noop)
    monkeypatch.setattr(http_entrypoint, "_flush_observability", noop)


@pytest.fixture
def managed_app():
    app = FastAPI()
    app.state.managed_task_drain_controller = ManagedTaskDrainController()
    app.include_router(http_entrypoint.router)
    return app


@pytest_asyncio.fixture
async def managed_client(managed_app):
    transport = httpx.ASGITransport(app=managed_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_handle_task_waits_for_task_completion_before_returning(
    monkeypatch, managed_client
):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_task(*args, **kwargs):
        started.set()
        await release.wait()
        return "done"

    monkeypatch.setattr(
        http_entrypoint, "_resolve_function", lambda func_ref: slow_task
    )

    response_task = asyncio.create_task(
        managed_client.post(
            "/handle_task",
            json={"func_ref": "pkg.slow_task", "args": [], "kwargs": {}},
        )
    )

    await asyncio.wait_for(started.wait(), timeout=1.0)
    assert not response_task.done()

    release.set()
    response = await asyncio.wait_for(response_task, timeout=1.0)

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "task": "pkg.slow_task"}


@pytest.mark.asyncio
async def test_handle_task_returns_503_when_instance_is_draining(
    managed_app, managed_client
):
    await managed_app.state.managed_task_drain_controller.start_draining("test")

    response = await managed_client.post(
        "/handle_task",
        json={"func_ref": "pkg.task", "args": [], "kwargs": {}},
    )

    assert response.status_code == 503
    assert response.json() == {"status": "DRAINING", "task": "pkg.task"}


@pytest.mark.asyncio
async def test_handle_task_releases_active_slot_on_failure(
    monkeypatch, managed_app, managed_client
):
    async def failing_task(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        http_entrypoint, "_resolve_function", lambda func_ref: failing_task
    )

    response = await managed_client.post(
        "/handle_task",
        json={"func_ref": "pkg.fail", "args": [], "kwargs": {}},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "FAILED"
    assert managed_app.state.managed_task_drain_controller.active_requests == 0


@pytest.mark.asyncio
async def test_kick_batched_respects_drain_controller(managed_app, managed_client):
    await managed_app.state.managed_task_drain_controller.start_draining("test")

    response = await managed_client.post(
        "/internal/feed/kick/batched",
        json={"tenant_id": "t1", "feed_id": "feed1", "group_name": "g1"},
    )

    assert response.status_code == 503
    assert response.json() == {
        "status": "DRAINING",
        "feed_id": "feed1",
        "group_name": "g1",
    }


@pytest.mark.asyncio
async def test_deliver_classic_respects_drain_controller(managed_app, managed_client):
    await managed_app.state.managed_task_drain_controller.start_draining("test")

    response = await managed_client.post(
        "/internal/feed/deliver/classic",
        json={
            "tenant_id": "t1",
            "feed_id": "feed1",
            "group_name": "g1",
            "dedupe_key": "dedupe-1",
            "timestamp": 123.0,
        },
    )

    assert response.status_code == 503
    assert response.json() == {
        "status": "DRAINING",
        "feed_id": "feed1",
        "dedupe_key": "dedupe-1",
    }

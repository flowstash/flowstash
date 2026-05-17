import asyncio
from contextlib import contextmanager

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from flowstash.config.runtime_config import RuntimeConfig

import flowstash.runtime.worker.backends.managed.http_entrypoint as http_entrypoint
from flowstash.runtime.worker.backends.managed.main import create_app
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


def test_create_app_wires_managed_routes_and_drain_controller():
    app = create_app(RuntimeConfig())

    assert isinstance(
        app.state.managed_task_drain_controller, ManagedTaskDrainController
    )
    routes = {route.path for route in app.router.routes}

    assert "/health" in routes
    assert "/handle_task" in routes
    assert "/internal/feed/kick/batched" in routes
    assert "/internal/feed/deliver/classic" in routes


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
        http_entrypoint, "_resolve_task_callable", lambda task_id, func_ref: slow_task
    )

    response_task = asyncio.create_task(
        managed_client.post(
            "/handle_task",
            json={"task_id": "pkg.slow_task"},
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
        json={"task_id": "pkg.task"},
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
        http_entrypoint,
        "_resolve_task_callable",
        lambda task_id, func_ref: failing_task,
    )

    response = await managed_client.post(
        "/handle_task",
        json={"task_id": "pkg.fail"},
    )

    assert response.status_code == 500
    assert response.json()["detail"]["status"] == "FAILED"
    assert managed_app.state.managed_task_drain_controller.active_requests == 0


@pytest.mark.asyncio
async def test_handle_task_returns_404_for_unknown_task_and_records_failure(
    monkeypatch, managed_client
):
    started_calls = []
    ended_calls = []

    async def record_started(**kwargs):
        started_calls.append(kwargs)

    async def record_ended(**kwargs):
        ended_calls.append(kwargs)

    monkeypatch.setattr(http_entrypoint, "record_run_started", record_started)
    monkeypatch.setattr(http_entrypoint, "record_run_ended", record_ended)

    response = await managed_client.post(
        "/handle_task",
        json={"task_id": "nonexistent.task"},
    )

    assert response.status_code == 404
    assert response.json()["status"] == "TASK_NOT_FOUND"
    assert len(started_calls) == 1
    assert started_calls[0]["entry_point"] == "nonexistent.task"
    assert started_calls[0]["attrs"] == {"args": {"args": [], "kwargs": {}}}
    assert len(ended_calls) == 1
    assert ended_calls[0]["status"] == "FAILED"
    assert ended_calls[0]["attrs"]["task_resolution_failed"] is True
    assert "Cannot resolve task" in ended_calls[0]["attrs"]["error"]


@pytest.mark.asyncio
async def test_handle_task_uses_flat_payload_fields(monkeypatch, managed_client):
    observed = {}

    @contextmanager
    def capture_integration_context(**kwargs):
        observed["context"] = kwargs

        class Ctx:
            corelation = object()

        yield Ctx()

    async def capture_task(*args, **kwargs):
        observed["call"] = {"args": list(args), "kwargs": kwargs}

    async def record_started(**kwargs):
        observed["started"] = kwargs

    monkeypatch.setattr(
        http_entrypoint, "integration_context", capture_integration_context
    )
    monkeypatch.setattr(http_entrypoint, "record_run_started", record_started)
    monkeypatch.setattr(
        http_entrypoint,
        "_resolve_task_callable",
        lambda task_id, func_ref: capture_task,
    )

    response = await managed_client.post(
        "/handle_task",
        json={
            "task_id": "452659e0-b429-47a1-93bd-5c3c0421e117",
            "task_name": "worker.tasks.discovery.discover_replenishment_needs_for_warehouse",
            "func_ref": "worker.tasks.discovery.discover_replenishment_needs_for_warehouse",
            "args": [],
            "kwargs": {
                "run_id": "03:00 20260517",
                "warehouse_code": "812",
            },
            "integration": "laa_aims",
            "pipeline": "rfid_replenishment",
            "triggered_by": "manual",
            "cron": None,
            "tags": {"source": "platform"},
            "delegation": {
                "parent_run_id": "67419c30-dc4c-49c9-9594-67563951fbf4",
                "operation_id": "a772ddba-bc2a-4a2c-ab12-99506bd83029",
                "target_task": "worker.tasks.discovery.discover_replenishment_needs_for_warehouse",
                "accepted_id": None,
                "schedule_time": None,
                "attrs": {},
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "task": "worker.tasks.discovery.discover_replenishment_needs_for_warehouse",
    }
    assert observed["call"] == {
        "args": [],
        "kwargs": {
            "run_id": "03:00 20260517",
            "warehouse_code": "812",
        },
    }
    assert observed["context"]["integration"] == "laa_aims"
    assert observed["context"]["integration_pipeline"] == "rfid_replenishment"
    assert (
        observed["context"]["parent_run_id"]
        == "67419c30-dc4c-49c9-9594-67563951fbf4"
    )
    assert (
        observed["context"]["operation_id"]
        == "a772ddba-bc2a-4a2c-ab12-99506bd83029"
    )
    assert observed["context"]["tags"] == {
        "source": "platform",
        "triggered_by": "manual",
    }
    assert observed["started"]["attrs"]["args"]["kwargs"] == {
        "run_id": "03:00 20260517",
        "warehouse_code": "812",
    }


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

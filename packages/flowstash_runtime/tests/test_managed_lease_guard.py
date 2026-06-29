"""
Tests for the lease idempotency guard in the managed HTTP entrypoint.

A fake lease client drives each acquire outcome and we assert the resulting
HTTP behaviour: ACQUIRED runs + releases, COMPLETED is skipped (200 duplicate),
BUSY/UNAVAILABLE return a retryable 503, and a disabled client runs unguarded.
"""

from contextlib import contextmanager

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

import flowstash.runtime.worker.backends.managed.http_entrypoint as http_entrypoint
import flowstash.runtime.worker.backends.managed.lease_client as lease_client
from flowstash.runtime.worker.backends.managed.drain import ManagedTaskDrainController
from flowstash.runtime.worker.backends.managed.lease_client import AcquireResult


@contextmanager
def _fake_integration_context(**kwargs):
    class Ctx:
        corelation = object()

    yield Ctx()


@pytest.fixture(autouse=True)
def patch_observability(monkeypatch):
    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(http_entrypoint, "integration_context", _fake_integration_context)
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


class FakeLease:
    def __init__(self, outcome):
        self.outcome = outcome
        self.released = []

    async def acquire(self, run_id, entry_point=None):
        return AcquireResult(self.outcome)

    async def release(self, run_id, status):
        self.released.append((run_id, status))


def _install_task(monkeypatch, fn):
    monkeypatch.setattr(
        http_entrypoint, "_resolve_task_callable", lambda task_id, func_ref: fn
    )


def _install_lease(monkeypatch, fake):
    monkeypatch.setattr(http_entrypoint, "get_lease_client", lambda: fake)


@pytest.mark.asyncio
async def test_acquired_runs_and_releases_succeeded(monkeypatch, managed_client):
    ran = []

    async def task(*a, **k):
        ran.append(True)

    _install_task(monkeypatch, task)
    fake = FakeLease("ACQUIRED")
    _install_lease(monkeypatch, fake)

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 200
    assert ran == [True]
    assert fake.released == [("r1", "SUCCEEDED")]


@pytest.mark.asyncio
async def test_completed_skips_execution(monkeypatch, managed_client):
    ran = []

    async def task(*a, **k):
        ran.append(True)

    _install_task(monkeypatch, task)
    _install_lease(monkeypatch, FakeLease("COMPLETED"))

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "duplicate"
    assert ran == []


@pytest.mark.asyncio
async def test_busy_returns_retryable_503(monkeypatch, managed_client):
    ran = []

    async def task(*a, **k):
        ran.append(True)

    _install_task(monkeypatch, task)
    _install_lease(monkeypatch, FakeLease("BUSY"))

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 503
    assert resp.json()["status"] == "LEASE_HELD"
    assert ran == []


@pytest.mark.asyncio
async def test_unavailable_fails_closed_503(monkeypatch, managed_client):
    _install_task(monkeypatch, lambda *a, **k: None)
    _install_lease(monkeypatch, FakeLease("UNAVAILABLE"))

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 503
    assert resp.json()["status"] == "LEASE_HELD"


@pytest.mark.asyncio
async def test_recovering_returns_503(monkeypatch, managed_client):
    # Broker restarting → refused/RECOVERING. Must be retryable (503), never run.
    ran = []

    async def task(*a, **k):
        ran.append(True)

    _install_task(monkeypatch, task)
    _install_lease(monkeypatch, FakeLease("RECOVERING"))

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 503
    assert resp.json()["status"] == "LEASE_HELD"
    assert ran == []


@pytest.mark.asyncio
async def test_acquired_task_failure_releases_failed(monkeypatch, managed_client):
    async def failing(*a, **k):
        raise RuntimeError("boom")

    _install_task(monkeypatch, failing)
    fake = FakeLease("ACQUIRED")
    _install_lease(monkeypatch, fake)

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 500
    assert fake.released == [("r1", "FAILED")]


@pytest.mark.asyncio
async def test_disabled_lease_runs_unguarded(monkeypatch, managed_client):
    ran = []

    async def task(*a, **k):
        ran.append(True)

    _install_task(monkeypatch, task)
    _install_lease(monkeypatch, None)  # get_lease_client returns None → guard off

    resp = await managed_client.post(
        "/handle_task", json={"task_id": "pkg.t", "run_id": "r1"}
    )
    assert resp.status_code == 200
    assert ran == [True]


# ── get_lease_client configuration gating ────────────────────────────────


def _reset_client_singleton(monkeypatch):
    monkeypatch.setattr(lease_client, "_resolved", False)
    monkeypatch.setattr(lease_client, "_client", None)


def test_get_lease_client_disabled_when_unconfigured(monkeypatch):
    _reset_client_singleton(monkeypatch)
    for var in ("LEASE_BROKER_URL", "MANAGED_API_URL", "FLOWSTASH_API_URL", "MANAGED_AUTH_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    assert lease_client.get_lease_client() is None


def test_get_lease_client_disabled_via_flag(monkeypatch):
    _reset_client_singleton(monkeypatch)
    monkeypatch.setenv("LEASE_BROKER_ENABLED", "false")
    monkeypatch.setenv("LEASE_BROKER_URL", "wss://api.example.com/ws/leases")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")
    assert lease_client.get_lease_client() is None


def test_get_lease_client_enabled_when_configured(monkeypatch):
    _reset_client_singleton(monkeypatch)
    monkeypatch.setenv("LEASE_BROKER_ENABLED", "true")
    monkeypatch.setenv("LEASE_BROKER_URL", "wss://api.example.com/ws/leases")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")
    client = lease_client.get_lease_client()
    assert client is not None
    assert client._url == "wss://api.example.com/ws/leases"


def test_derive_broker_url_from_https_api(monkeypatch):
    _reset_client_singleton(monkeypatch)
    monkeypatch.setenv("LEASE_BROKER_ENABLED", "true")
    monkeypatch.delenv("LEASE_BROKER_URL", raising=False)
    monkeypatch.setenv("MANAGED_API_URL", "https://api.flowstash.dev")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")
    client = lease_client.get_lease_client()
    assert client is not None
    assert client._url == "wss://api.flowstash.dev/ws/leases"

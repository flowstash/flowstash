"""
Tests for ManagedTasksBackend and ManagedFeedBackend.

Uses respx to mock httpx HTTP calls and verifies correct
API payloads/endpoints are called.
"""

import pytest
import httpx
import respx
from unittest.mock import MagicMock

from flowstash.queue.backends.managed_tasks import ManagedTasksBackend, ManagedJobHandle
from flowstash.queue.backend import Schedule
from flowstash.context import IntegrationContext
from flowstash.pipelines.backends.managed_feed import ManagedFeedBackend
from flowstash.pipelines.records_model import RecordData

# ─── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def backend():
    """Create a ManagedTasksBackend pointed at a mock API."""
    return ManagedTasksBackend(
        api_url="https://api.test.integrator.com",
        auth_token="test-jwt-token",
        service_url="https://tenant-abc.run.app",
        project_id="test-project",
        environment="test",
    )


def _dummy_func():
    """A dummy function for testing."""
    pass


# ─── ManagedTasksBackend._derive_task_id ─────────────────────────────


def test_derive_task_id_plain_function(backend):
    """_derive_task_id() should return module.name for a plain function."""
    task_id = backend._derive_task_id(_dummy_func)
    assert task_id == f"{_dummy_func.__module__}._dummy_func"


def test_derive_task_id_task_wrapper(backend):
    """_derive_task_id() should unwrap TaskWrapper via .func attribute."""
    from types import SimpleNamespace

    fake_func = SimpleNamespace(__module__="my.module", __name__="my_task")

    class FakeWrapper:
        func = fake_func

    task_id = backend._derive_task_id(FakeWrapper())
    assert task_id == "my.module.my_task"


def test_derive_task_id_is_consistent_across_submit_and_register_schedule(backend):
    """submit() and register_schedule() should produce the same task_id."""
    expected = backend._derive_task_id(_dummy_func)

    # register_schedule stores task_id in _registered_tasks
    from flowstash.queue.backend import Schedule

    backend.register_schedule(_dummy_func, Schedule(cron="0 * * * *"))
    registered_id = backend._registered_tasks[-1]["task_id"]
    assert registered_id == expected


# ─── ManagedTasksBackend.submit ──────────────────────────────────────


@respx.mock
def test_submit_sends_correct_request(backend):
    """submit() should POST to /v1/tasks/submit with correct payload."""
    route = respx.post("https://api.test.integrator.com/v1/tasks/submit").mock(
        return_value=httpx.Response(
            200,
            json={"task_id": "task-123", "status": "submitted"},
        )
    )

    result = backend.submit(
        func=_dummy_func,
        args=(1, "hello"),
        kwargs={"key": "value"},
        integration="test-integration",
        pipeline="test-pipeline",
    )

    assert route.called
    assert isinstance(result, ManagedJobHandle)
    assert result.id == "task-123"

    # Verify the request payload
    request = route.calls[0].request
    assert request.headers["Authorization"] == "Bearer test-jwt-token"
    assert request.headers["Content-Type"] == "application/json"


@respx.mock
def test_submit_with_context(backend):
    """submit() should propagate context data."""
    route = respx.post("https://api.test.integrator.com/v1/tasks/submit").mock(
        return_value=httpx.Response(
            200,
            json={"task_id": "task-456", "status": "submitted"},
        )
    )

    ctx = IntegrationContext(
        integration="ctx-integration",
        integration_pipeline="ctx-pipeline",
        run_id="run-abc",
    )

    result = backend.submit(
        func=_dummy_func,
        args=(),
        kwargs={},
        context=ctx,
    )

    assert route.called
    assert result.id == "task-456"


@respx.mock
def test_submit_raises_on_http_error(backend):
    """submit() should raise on non-2xx responses."""
    respx.post("https://api.test.integrator.com/v1/tasks/submit").mock(
        return_value=httpx.Response(500, json={"detail": "Internal error"})
    )

    with pytest.raises(httpx.HTTPStatusError):
        backend.submit(func=_dummy_func, args=(), kwargs={})


# ─── ManagedTasksBackend.schedule ────────────────────────────────────


@respx.mock
def test_schedule_sends_with_schedule_time(backend):
    """schedule() should include schedule_time in the request."""
    route = respx.post("https://api.test.integrator.com/v1/tasks/submit").mock(
        return_value=httpx.Response(
            200,
            json={"task_id": "sched-789", "status": "submitted"},
        )
    )

    result = backend.schedule(
        func=_dummy_func,
        args=(),
        kwargs={},
        eta_or_delay=5000,  # 5 seconds in ms
    )

    assert route.called
    assert result.id == "sched-789"

    # Verify schedule_time is present in payload
    import json

    body = json.loads(route.calls[0].request.content)
    assert body.get("schedule_time") is not None


# ─── ManagedTasksBackend.register_schedule ───────────────────────────


# ─── ManagedTasksBackend.get_scheduled_jobs ──────────────────────────


def test_get_scheduled_jobs_returns_empty(backend):
    """get_scheduled_jobs() should return empty — schedules are server-side."""
    assert backend.get_scheduled_jobs() == []


# ─── ManagedJobHandle ────────────────────────────────────────────────


def test_job_handle_properties():
    """ManagedJobHandle should have correct defaults."""
    handle = ManagedJobHandle("test-id", tags={"foo": "bar"})
    assert handle.id == "test-id"
    assert handle.tags == {"foo": "bar"}
    assert handle.status() == "submitted"
    assert handle.cancel() is False


@pytest.mark.asyncio
async def test_job_handle_result_raises():
    """ManagedJobHandle.result() should raise NotImplementedError."""
    handle = ManagedJobHandle("test-id")
    with pytest.raises(NotImplementedError):
        await handle.result()


# ─── ManagedFeedBackend ──────────────────────────────────────────────


@pytest.fixture
def feed_backend():
    """Create a ManagedFeedBackend pointed at a mock API."""
    return ManagedFeedBackend(
        api_url="https://api.test.integrator.com",
        auth_token="test-jwt-token",
        project_id="test-project",
        environment="test",
    )


@respx.mock
@pytest.mark.asyncio
async def test_feed_backend_publish(feed_backend):
    """publish() should POST to the publish endpoint with correct payload."""
    route = respx.post("https://api.test.integrator.com/v1/feed/feed-123/publish").mock(
        return_value=httpx.Response(
            200,
            json={"status": "published"},
        )
    )

    record = RecordData(record_id="rec-abc", record_type="user", data={"name": "test"})
    result = await feed_backend.publish("feed-123", record)

    assert route.called
    assert result == "published"

    request = route.calls[0].request
    assert request.headers["Authorization"] == "Bearer test-jwt-token"
    assert request.headers["Content-Type"] == "application/json"

    import json

    body = json.loads(request.content)
    assert body["data"] == {"name": "test"}
    assert "timestamp" in body

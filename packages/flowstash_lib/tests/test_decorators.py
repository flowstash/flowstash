import gc
import pytest
import asyncio
from datetime import datetime, UTC
from unittest.mock import MagicMock
from flowstash.decorators import integration_step, integration_task
from flowstash.context import current_context, integration_context
from flowstash.queue.backend import set_backend, Schedule


@pytest.mark.asyncio
async def test_integration_step_async():
    @integration_step(integration="test", integration_pipeline="pipe", tags={"t": "v"})
    async def my_step():
        ctx = current_context()
        assert ctx.integration == "test"
        assert ctx.tags == {"t": "v"}
        return "ok"

    result = await my_step()
    assert result == "ok"


def test_integration_step_sync():
    @integration_step(integration="test", integration_pipeline="pipe", tags={"s": "w"})
    def my_step():
        ctx = current_context()
        assert ctx.integration == "test"
        assert ctx.tags == {"s": "w"}
        return "ok"

    result = my_step()
    assert result == "ok"


@pytest.mark.asyncio
async def test_integration_task_run():
    @integration_task(
        integration="test", integration_pipeline="pipe", tags={"task": "true"}
    )
    async def my_task():
        ctx = current_context()
        assert ctx.tags == {"task": "true"}
        return 42

    result = await my_task.run()
    assert result == 42


def test_integration_task_submit():
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(
        integration="test", integration_pipeline="pipe", tags={"priority": "high"}
    )
    def my_task(x, y):
        return x + y

    with integration_context(
        integration="outer", integration_pipeline="outer_pipe", tags={"global": "1"}
    ):
        my_task.submit(1, 2)

    mock_backend.submit.assert_called_once()
    args, kwargs = mock_backend.submit.call_args
    # fw.is_subtask is no longer added; delegation metadata is passed separately
    assert kwargs["tags"] == {"priority": "high"}
    assert kwargs["context"].tags == {"global": "1"}
    # delegation carries the causal envelope
    delegation = kwargs.get("delegation")
    assert delegation is not None
    assert delegation.target_task == "test_decorators.my_task"
    assert delegation.parent_run_id is not None
    assert delegation.operation_id is not None


def test_integration_task_schedule():
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(
        integration="test", integration_pipeline="pipe", tags={"scheduled": "true"}
    )
    def my_task():
        pass

    my_task.schedule(60)
    mock_backend.schedule.assert_called_once()
    args, kwargs = mock_backend.schedule.call_args
    assert kwargs["eta_or_delay"] == 60
    # fw.is_subtask is no longer added; delegation carries causal metadata
    assert kwargs["tags"] == {"scheduled": "true"}
    delegation = kwargs.get("delegation")
    assert delegation is not None
    assert delegation.target_task == "test_decorators.my_task"


def test_integration_task_default_schedule():
    mock_backend = MagicMock()

    # Reset pending schedules for a clean test
    from flowstash.queue.backend import _pending_schedules

    _pending_schedules.clear()
    # Also reset the global backend
    import flowstash.queue.backend as backend_module

    backend_module._backend = None

    # Define task with schedule BEFORE setting backend (tests deferred registration)
    @integration_task(
        integration="test",
        integration_pipeline="pipe",
        default_schedule=Schedule(cron="0 * * * *"),
    )
    def my_scheduled_task():
        pass

    # 1. Backend shouldn't have been called yet
    mock_backend.register_schedule.assert_not_called()

    # 2. Set backend, should trigger deferred registration
    set_backend(mock_backend)

    mock_backend.register_schedule.assert_called_once()
    args, kwargs = mock_backend.register_schedule.call_args
    # register_schedule receives the TaskWrapper, not the raw function
    assert args[0] is my_scheduled_task
    assert args[1].cron == "0 * * * *"


def test_integration_task_bare_call_submits():
    """A bare my_task(...) statement still enqueues at end of statement."""
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(integration="test", integration_pipeline="pipe")
    def my_task(x):
        return x

    my_task(41)  # result discarded -> finalizer dispatches immediately on CPython
    mock_backend.submit.assert_called_once()
    args, kwargs = mock_backend.submit.call_args
    assert args[1] == (41,)


def test_integration_task_chained_schedule():
    """my_task(args).schedule(eta_or_delay=...) dispatches once, as a scheduled job."""
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(integration="test", integration_pipeline="pipe")
    def my_task(x, y):
        return x + y

    handle = my_task(1, 2).schedule(eta_or_delay=25 * 60)

    mock_backend.schedule.assert_called_once()
    mock_backend.submit.assert_not_called()
    assert handle is mock_backend.schedule.return_value

    args, kwargs = mock_backend.schedule.call_args
    assert args[1] == (1, 2)
    assert kwargs["eta_or_delay"] == 25 * 60
    # schedule_time metadata is interpreted in seconds
    delegation = kwargs["delegation"]
    assert delegation.schedule_time is not None
    delta = (delegation.schedule_time - datetime.now(UTC)).total_seconds()
    assert 25 * 60 - 5 < delta < 25 * 60 + 5

    # GC of the invocation must not double-dispatch
    gc.collect()
    mock_backend.submit.assert_not_called()
    mock_backend.schedule.assert_called_once()


def test_invocation_handle_proxy_submits_once():
    """Using the returned handle triggers exactly one submit."""
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(integration="test", integration_pipeline="pipe")
    def my_task():
        pass

    h = my_task()
    mock_backend.submit.assert_not_called()  # lazy while the reference is held
    assert "pending" in repr(h)

    _ = h.id
    _ = h.status()
    mock_backend.submit.assert_called_once()
    assert "dispatched" in repr(h)
    assert h.id is mock_backend.submit.return_value.id


def test_invocation_schedule_after_use_raises():
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(integration="test", integration_pipeline="pipe")
    def my_task():
        pass

    h = my_task()
    _ = h.id  # forces a submit
    with pytest.raises(RuntimeError, match="already"):
        h.schedule(eta_or_delay=60)
    mock_backend.schedule.assert_not_called()


def test_pending_invocations_flushed_at_run_exit():
    """Invocations held but never used are dispatched when the root run exits."""
    mock_backend = MagicMock()
    set_backend(mock_backend)

    @integration_task(integration="test", integration_pipeline="pipe")
    def my_task():
        pass

    holder = []
    with integration_context(integration="outer", integration_pipeline="op"):
        holder.append(my_task())  # strong ref: no finalizer until flush
        mock_backend.submit.assert_not_called()

    mock_backend.submit.assert_called_once()
    with pytest.raises(RuntimeError, match="already"):
        holder[0].schedule(eta_or_delay=5)

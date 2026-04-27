import pytest
import asyncio
from unittest.mock import MagicMock, patch
from flowstash.ingress import ingress
from flowstash.queue.backend import Schedule, _pending_schedules
from flowstash.context import integration_context

@pytest.fixture(autouse=True)
def clear_registry():
    import flowstash.queue.backend as backend_module
    backend_module._backend = None
    ingress._webhooks.clear()
    _pending_schedules.clear()
    yield

def test_webhook_registration():
    @ingress.webhook(pipeline="p1", integration="i1", path="/p1", method="POST")
    def handler1(payload):
        return "ok"

    @ingress.webhook(pipeline="p2", integration="i1", path="/p2", method="custom")
    def handler2(payload):
        return "ok"

    webhooks = ingress.get_webhooks()
    assert len(webhooks) == 2
    assert handler1 in webhooks
    assert handler2 in webhooks
    assert handler1._ingress_metadata["pipeline"] == "p1"
    assert handler2._ingress_metadata["pipeline"] == "p2"

@pytest.mark.asyncio
async def test_poll_registration_and_execution():
    # 1. Registration
    @ingress.poll(pipeline="poll_pipeline", integration="i1", schedule="0 0 * * *", name="my_poll")
    async def poll_handler(state):
        state["last_run"] = "now"
        return "done"

    # Verify TaskWrapper was created
    from flowstash.decorators import TaskWrapper
    assert isinstance(poll_handler, TaskWrapper)
    
    # Verify schedule was registered in pending
    assert len(_pending_schedules) == 1
    func, schedule, args, kwargs, tags = _pending_schedules[0]
    assert isinstance(schedule, Schedule)
    assert schedule.cron == "0 0 * * *"
    
    # Verify schedule with Schedule object is also supported
    _pending_schedules.clear()
    schedule_obj = Schedule(cron="0 1 * * *")
    @ingress.poll(pipeline="poll_pipeline", integration="i1", schedule=schedule_obj, name="my_poll_2")
    async def poll_handler_2(state):
        return "done2"
        
    assert len(_pending_schedules) == 1
    _, sched2, _, _, _ = _pending_schedules[0]
    assert isinstance(sched2, Schedule)
    assert sched2.cron == "0 1 * * *"
    
    # 2. Execution (State Persistence)
    # We need a context to run the poll
    with integration_context(integration="i1", integration_pipeline="poll_pipeline", tenant_id="t1"):
        # Mock load/save to verify they are called
        with patch.object(ingress, "_load_state", return_value={}) as mock_load, \
             patch.object(ingress, "_save_state") as mock_save:
            
            result = await poll_handler.run()

            assert result == "done"
            mock_load.assert_called_once()
            mock_save.assert_called_once()
            # Verify state was updated and saved
            # New signature: _save_state(pipeline, ingress_key, state)
            saved_state = mock_save.call_args[0][2]
            assert saved_state["last_run"] == "now"

def test_poll_sync_handler():
    @ingress.poll(pipeline="p1", integration="i1", schedule="* * * * *")
    def sync_poll(state):
        state["count"] = state.get("count", 0) + 1
        return state["count"]

    with integration_context(integration="i1", integration_pipeline="p1", tenant_id="t1"):
        with patch.object(ingress, "_load_state", return_value={"count": 5}), \
             patch.object(ingress, "_save_state") as mock_save:
            
            # Since ingress.poll always wraps in async state_wrapper, we must await it via .run() 
            # OR we can just call it if we want to run inline, but .run() is cleaner
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(sync_poll.run())

            assert result == 6
            # New signature: _save_state(pipeline, ingress_key, state)
            saved_state = mock_save.call_args[0][2]
            assert saved_state["count"] == 6

import pytest
import dramatiq
from flowstash.runtime.worker.backends.dramatiq.dramatiq_backend import DramatiqBackend
from flowstash.context import integration_context

def test_dramatiq_backend_prepare_headers():
    backend = DramatiqBackend()
    
    with integration_context(integration="test", integration_pipeline="pipe", tags={"global": "v1"}) as ctx:
        headers = backend._prepare_headers(ctx, tags={"local": "v2"})
        assert headers["fw.integration"] == "test"
        assert headers["fw.pipeline"] == "pipe"
        assert headers["fw.run_id"] == ctx.run_id
        assert headers["fw.tags"] == {"global": "v1", "local": "v2"}

@dramatiq.actor
def my_actor(*args, **kwargs):
    pass

def test_dramatiq_backend_submit():
    backend = DramatiqBackend()
    # Mocking actor.send_with_options would be better, but we can just check if it doesn't crash
    # and returns a handle.
    
    with integration_context(integration="test", integration_pipeline="pipe"):
        handle = backend.submit(my_actor, (1, 2), {}, tags={"prio": "high"})
        assert handle.id is not None
        assert handle.tags == {"prio": "high", "fw.is_subtask": False}

def test_scheduled_job_tracking():
    from flowstash.queue.backend import Schedule
    
    backend = DramatiqBackend()
    schedule = Schedule(cron="0 * * * *")
    
    # Register a schedule
    backend.register_schedule(my_actor, schedule)
    jobs = backend.get_scheduled_jobs()
    
    assert len(jobs) == 1
    job = jobs[0]
    
    # Verify dedicated fields (not in tags)
    # Actor objects wrap the function, need to access underlying fn
    expected_id = f"{my_actor.fn.__module__}.{my_actor.fn.__name__}"
    assert job.scheduled_job_id == expected_id
    assert job.schedule == "0 * * * *"
    assert "fw.scheduled_job_id" not in job.tags  # Should NOT be in tags
    
    # Verify ID format
    assert job.id == f"scheduled:{job.scheduled_job_id}"

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
        # fw.run_id is intentionally NOT forwarded: execution side allocates a fresh run_id
        assert "fw.run_id" not in headers
        assert headers["fw.tags"] == {"global": "v1", "local": "v2"}

@dramatiq.actor
def my_actor(*args, **kwargs):
    pass

def test_dramatiq_backend_submit():
    backend = DramatiqBackend()
    
    with integration_context(integration="test", integration_pipeline="pipe"):
        handle = backend.submit(my_actor, (1, 2), {}, tags={"prio": "high"})
        assert handle.id is not None
        # fw.is_subtask removed; delegation carries causal metadata instead
        assert handle.tags == {"prio": "high"}

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

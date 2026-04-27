import pytest
from flowstash.integration.state import State
from flowstash.context import IntegrationContext, integration_context

def test_state_outside_context_raises():
    with pytest.raises(RuntimeError, match="State used outside of an integration run"):
        State.get("foo")

def test_state_basic_roundtrip():
    ctx = IntegrationContext(integration="test-integration", integration_pipeline="test-pipeline", run_id="test-run")
    with State.use(ctx):
        State.set("my_key", {"val": 123})
        assert State.get("my_key") == {"val": 123}

def test_state_scope_integration():
    ctx = IntegrationContext(integration="int-a", integration_pipeline="pipe-a", run_id="run-1")
    with State.use(ctx):
        State.set("k", "val-a", scope="integration")
        assert State.get("k", scope="integration") == "val-a"
        
    ctx_b = IntegrationContext(integration="int-b", integration_pipeline="pipe-b", run_id="run-2")
    with State.use(ctx_b):
        assert State.get("k", scope="integration") is None
        State.set("k", "val-b", scope="integration")
        assert State.get("k", scope="integration") == "val-b"

def test_state_scope_pipeline():
    ctx = IntegrationContext(integration="int", integration_pipeline="pipe-1", run_id="run-1")
    with State.use(ctx):
        State.set("k", "p1", scope="pipeline")
        assert State.get("k", scope="pipeline") == "p1"
        
    ctx_2 = IntegrationContext(integration="int", integration_pipeline="pipe-2", run_id="run-2")
    with State.use(ctx_2):
        assert State.get("k", scope="pipeline") is None

def test_state_scope_ingress():
    ctx = IntegrationContext(
        integration="int", 
        integration_pipeline="pipe", 
        run_id="run", 
        ingress_name="my-poll"
    )
    with State.use(ctx):
        State.set("last_id", 100, scope="ingress")
        assert State.get("last_id", scope="ingress") == 100

    # Without ingress_name, it should raise
    ctx_no_ingress = IntegrationContext(integration="int", integration_pipeline="pipe", run_id="run")
    with State.use(ctx_no_ingress):
        with pytest.raises(RuntimeError, match="ingress_name missing"):
            State.get("last_id", scope="ingress")

def test_integration_context_integration():
    """Verify that State facade picks up context from integration_context."""
    with integration_context(integration="my-int", integration_pipeline="my-pipe"):
        State.set("hello", "world")
        assert State.get("hello") == "world"

def test_state_get_entry():
    ctx = IntegrationContext(integration="test", integration_pipeline="pipe", run_id="run")
    with State.use(ctx):
        State.set("foo", "bar")
        entry = State.get_entry("foo")
        assert entry.value == b'"bar"'
        assert entry.content_type == "application/json"

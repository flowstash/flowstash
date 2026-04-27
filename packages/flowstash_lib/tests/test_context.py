import pytest
from flowstash.context import IntegrationContext, integration_context, current_context

def test_integration_context_creation():
    with integration_context(integration="test_app", integration_pipeline="test_pipe", tags={"env": "prod"}) as ctx:
        assert ctx.integration == "test_app"
        assert ctx.integration_pipeline == "test_pipe"
        assert ctx.run_id is not None
        assert ctx.tags == {"env": "prod"}
        assert current_context() == ctx

def test_integration_context_nesting():
    with integration_context(integration="parent", integration_pipeline="p_pipe", tags={"region": "us"}) as p_ctx:
        parent_run_id = p_ctx.run_id
        with integration_context(integration_pipeline="child_pipe", tags={"env": "dev"}) as c_ctx:
            assert c_ctx.integration == "parent"  # Inherited
            assert c_ctx.integration_pipeline == "child_pipe"  # Overridden
            assert c_ctx.run_id == parent_run_id  # Inherited
            assert c_ctx.tags == {"region": "us", "env": "dev"}  # Merged
            assert current_context() == c_ctx
        assert current_context() == p_ctx

def test_integration_context_reset():
    assert current_context() is None
    with integration_context(integration="test", integration_pipeline="pipe"):
        pass
    assert current_context() is None

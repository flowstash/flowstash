import asyncio
import os
import shutil
import pytest
import httpx

from flowstash.queue.backend import set_backend
from flowstash.queue.asyncio_backend import AsyncioBackend
from flowstash.decorators import integration_task, integration_step
from flowstash.context import current_context
from flowstash.observability.ingestion import set_observability_config
from flowstash.config.observability_config import ObservabilityConfig, StoreType, DurabilityMode

from flowstash.clients import ClientConfigRegistry, HttpClient

# 1. Define testing client config for httpbin.org
CLIENT_CONFIG = {
    "clients": {
        "httpbin": {
            "integration": "testing_integration",
            "baseUrl": "https://httpbin.org",
            "timeout": 10.0,
            "maxRetries": 1
        }
    }
}

registry = ClientConfigRegistry(CLIENT_CONFIG)

# Configure observability directory
OBS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

def configure_observability():
    # Configure observability to use local file store for verification
    if os.path.exists(OBS_DIR):
        shutil.rmtree(OBS_DIR)
    os.makedirs(OBS_DIR)

    obs_config = ObservabilityConfig(
        durability=DurabilityMode.IMMEDIATE,
        store_type=StoreType.LOCAL,
        local_store_path=OBS_DIR
    )
    set_observability_config(obs_config)

@pytest.fixture(autouse=True)
def setup_observability():
    configure_observability()
    yield

# 2. Define integration tasks
@integration_step(
    integration="testing_integration",
    integration_pipeline="comprehensive_test_pipeline",
    name="sub_step_logic"
)
async def sub_step_task(input_val: str):
    """A sub-step to test span event recording."""
    ctx = current_context()
    assert ctx.integration_pipeline == "comprehensive_test_pipeline"
    return f"processed: {input_val}"

@integration_task(
    integration="testing_integration",
    integration_pipeline="comprehensive_test_pipeline",
    name="nested_task_logic"
)
async def nested_task(input_val: str):
    """A nested task to verify it is treated as a span of the main run."""
    ctx = current_context()
    return f"nested: {input_val}"

@integration_task(
    integration="testing_integration",
    integration_pipeline="comprehensive_test_pipeline",
    tags={"env": "test", "priority": "high"}
)
async def fetch_data_task(path: str):
    """
    A worker task that performs a real HTTP call to httpbin.
    It verifies that the correlation context is correctly available.
    """
    ctx = current_context()
    assert ctx is not None
    assert ctx.integration == "testing_integration"
    assert ctx.integration_pipeline == "comprehensive_test_pipeline"
    assert ctx.tags.get("env") == "test"
    
    # Initialize HTTP client using the registry
    client = HttpClient("httpbin", registry["httpbin"])
    
    # Perform a sub-step
    sub_result = await sub_step_task("hello")
    await asyncio.sleep(1)  
    assert sub_result == "processed: hello"
    
    # Perform a nested task (worker-style submit)
    # The user says: "unless we call another sub task from exiting task... we treat that as span"
    # Actually, calling .run() or .submit() from within?
    # Usually, if we await a task, it's a span.
    # If we submit a task, it's also a span if it's part of the same run.
    nested_result = await nested_task.run("world")
    await asyncio.sleep(1)  
    assert nested_result == "nested: world"
    response = await client.request("GET", path)
    
    # Return some info to verify
    return {
        "status": response.status_code,
        "url": str(response.url),
        "run_id": ctx.run_id,
        "trace_id": ctx.corelation.trace_id if ctx.corelation else None
    }

@pytest.mark.asyncio
async def test_worker_execution_with_asyncio_backend():
    # Setup AsyncioBackend for immediate execution
    backend = AsyncioBackend()
    set_backend(backend)
    
    # Submit the task (worker style)
    # This will return a JobHandle immediately
    handle = fetch_data_task.submit(path="/get")
    
    # In AsyncioBackend, it's already running in the background loop
    assert handle.id is not None
    
    # Wait for the result
    result = await handle.result()
    
    # Verify the results
    assert result["status"] == 200
    assert "https://httpbin.org/get" in result["url"]
    assert result["run_id"] is not None
    
    print(f"\nTask finished successfully!")
    print(f"Run ID: {result['run_id']}")
    print(f"URL: {result['url']}")
    
    # Wait for async observability jobs to finish
    await asyncio.sleep(2)
    
    # Verify that run events were created in the file store
    from datetime import datetime, UTC
    date_str = datetime.now(UTC).date().isoformat()
    run_dir = os.path.join(OBS_DIR, f"{date_str}_{result['run_id']}")
    assert os.path.exists(run_dir), f"Run directory {run_dir} not found"
    
    # In FileStore, events are directly in the run directory
    event_files = os.listdir(run_dir)
    print(f"Found event files: {event_files}")
    
    run_started = any("RUN_STARTED" in f for f in event_files)
    run_ended = any("RUN_ENDED" in f for f in event_files)
    run_scheduled = any("RUN_SCHEDULED" in f for f in event_files)
    
    def event_has_name(filename, name):
        with open(os.path.join(run_dir, filename), "r") as f:
            content = f.read()
            return name in content

    span_started = any("SPAN_STARTED" in f and event_has_name(f, "sub_step_logic") for f in event_files)
    span_ended = any("SPAN_ENDED" in f and event_has_name(f, "sub_step_logic") for f in event_files)
    
    # Nested task events (should be spans)
    nested_span_started = any("SPAN_STARTED" in f and event_has_name(f, "nested_task_logic") for f in event_files)
    nested_span_ended = any("SPAN_ENDED" in f and event_has_name(f, "nested_task_logic") for f in event_files)
    # Root run events for nested task should NOT exist (as RUN_STARTED)
    nested_run_started = any("RUN_STARTED" in f and event_has_name(f, "nested_task_logic") for f in event_files)
    
    assert run_scheduled, "Run SCHEDULED event missing"
    assert run_started, "Run STARTED event missing"
    assert run_ended, "Run ENDED event missing"
    assert span_started, "Step Span STARTED event missing"
    assert span_ended, "Step Span ENDED event missing"
    assert nested_span_started, "Nested Task Span STARTED event missing"
    assert nested_span_ended, "Nested Task Span ENDED event missing"
    assert not nested_run_started, "Nested Task should NOT have its own Run event"
    
    print("Verification of observability events SUCCESSFUL!")

if __name__ == "__main__":
    # Allow running this script directly for demonstration
    async def run_demo():
        print("Starting comprehensive worker task demo...")
        configure_observability()
        await test_worker_execution_with_asyncio_backend()
        print("Demo completed.")

    asyncio.run(run_demo())

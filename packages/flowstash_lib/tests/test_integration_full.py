
import pytest
import asyncio
import json
import os
import time
import httpx
from typing import List, Dict, Any
from fastapi.testclient import TestClient
from flowstash.ingress import ingress
from flowstash.pipelines.records_feed import RecordsFeed
from flowstash.pipelines.consumer import feed_consumer, ConsumerRunner
from flowstash.pipelines.records_model import RecordData
from flowstash.runtime.ingress.app import create_fastapi_app
from flowstash.config.runtime_config import RuntimeConfig, WebhooksConfig, BackendConfig, BackendType
from flowstash.pipelines.redis_client import get_redis_client

# --- Test Data & Paths ---
OUTPUT_DIR = "/tmp/framework_test_output"
SINGLE_OUTPUT = f"{OUTPUT_DIR}/single.jsonl"
BATCH_OUTPUT = f"{OUTPUT_DIR}/batch.jsonl"
FEED_ID = "integration_test_feed"

# Ensure clean state for output
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def clean_outputs():
    if os.path.exists(SINGLE_OUTPUT):
        os.remove(SINGLE_OUTPUT)
    if os.path.exists(BATCH_OUTPUT):
        os.remove(BATCH_OUTPUT)

# --- Webhooks & Consumers ---

from fastapi import Request

@ingress.webhook(integration="test_integration", pipeline="feed_publisher", path="/test_integration/feed_publisher")
async def publisher_webhook(request: Request):
    """
    Webhook that receives data and publishes to RecordsFeed.
    """
    request_data = await request.json()
    feed = RecordsFeed.get(FEED_ID)
    
    # Create a record from the request
    record = RecordData(
        record_id=request_data["id"],
        record_type="test_event",
        data=request_data,
        dedupe_key=request_data["id"]
    )
    
    await feed.publish(record)
    return {"status": "published", "id": request_data["id"]}


@feed_consumer(feed_id=FEED_ID, batch=False, subscription="test_single_sub")
async def single_consumer(record: RecordData):
    """
    Consumer a) non batch consumer
    """
    with open(SINGLE_OUTPUT, "a") as f:
        f.write(json.dumps({
            "record_id": record.record_id,
            "data": record.data,
            "ts": time.time()
        }) + "\n")


@feed_consumer(feed_id=FEED_ID, batch=True, max_batch_size=2, subscription="test_batch_sub", max_delay_ms=1000)
async def batch_consumer(records: List[RecordData]):
    """
    Consumer b) batch consumer with max size 2
    """
    with open(BATCH_OUTPUT, "a") as f:
        f.write(json.dumps({
            "batch_size": len(records),
            "record_ids": [r.record_id for r in records],
            "ts": time.time()
        }) + "\n")


# --- Test Infrastructure ---

async def is_redis_available():
    try:
        client = get_redis_client()
        await client.ping()
        return True
    except Exception:
        return False

@pytest.mark.asyncio
async def test_full_webhook_to_consumer_flow():
    # 0. Check Redis
    if not await is_redis_available():
        pytest.skip("Redis not available")

    # 1. Setup
    clean_outputs()
    
    # Ensure webhook is registered (in case another test cleared it)
    if publisher_webhook not in ingress.get_webhooks():
        ingress._webhooks.append(publisher_webhook)
    
    # Create minimal config for Ingress App
    config = RuntimeConfig(
        webhooks=WebhooksConfig(prefix="/webhooks"),
        backend=BackendConfig(type=BackendType.DRAMATIQ)
    )
    
    # 2. Spin up FastAPI (using AsyncClient for same-loop execution)
    from httpx import AsyncClient, ASGITransport
    app = create_fastapi_app(config)
    
    # Verify the backend was set correctly by the wiring
    from flowstash.queue.backend import get_backend
    from flowstash.runtime.worker.backends.dramatiq.dramatiq_backend import DramatiqBackend
    try:
        backend = get_backend()
        assert isinstance(backend, DramatiqBackend), "Expected DramatiqBackend to be configured"
    except Exception as e:
        pytest.fail(f"Backend configuration failed: {e}")
    
    # 3. Call webhook 5 times
    # Records: 1, 2, 3, 4, 5
    print("Sending 5 webhooks...")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for i in range(1, 6):
            payload = {"id": f"rec_{i}", "msg": f"hello {i}"}
            resp = await client.post("/webhooks/test_integration/feed_publisher", json=payload)
            assert resp.status_code == 200
            assert resp.json()["status"] == "published"
        
    # 4. Spin up ConsumerRunner
    print("Starting ConsumerRunner...")
    runner = ConsumerRunner(tenant_id="test_tenant")
    
    # Run the consumer loop for a brief period to process events
    # We purposefully don't use 'await runner.start()' directly because it blocks forever.
    # We'll wrap it in a task and cancel it after some time.
    
    runner_task = asyncio.create_task(runner.start())
    
    # Wait for processing
    # Batch consumer has max_delay of 1s, so we need > 1s.
    # We expect:
    # Batch 1: 2 records
    # Batch 2: 2 records
    # Batch 3: 1 record (after timeout)
    print("Waiting for consumers to process...")
    await asyncio.sleep(3.0) 
    
    # Stop runner
    await runner.stop()
    try:
        await runner_task
    except asyncio.CancelledError:
        pass
        
    print("Verifying outputs...")
    
    # 5. Verify Outputs
    
    # Verify Single Consumer (a)
    assert os.path.exists(SINGLE_OUTPUT), "Single consumer output file missing"
    with open(SINGLE_OUTPUT, "r") as f:
        lines = f.readlines()
        assert len(lines) == 5, f"Expected 5 single records, got {len(lines)}"
        
        # Verify content of first
        first = json.loads(lines[0])
        assert first["record_id"] == "rec_1"
        assert first["data"]["msg"] == "hello 1"

    # Verify Batch Consumer (b)
    # Expected batches: [2, 2, 1] (total 5)
    assert os.path.exists(BATCH_OUTPUT), "Batch consumer output file missing"
    with open(BATCH_OUTPUT, "r") as f:
        lines = f.readlines()
        batches = [json.loads(line) for line in lines]
        
        total_records = sum(b["batch_size"] for b in batches)
        assert total_records == 5, f"Expected 5 total records in batches, got {total_records}"
        
        # We expect at least one full batch of 2
        full_batches = [b for b in batches if b["batch_size"] == 2]
        assert len(full_batches) >= 2, "Expected at least 2 full batches"
        
        # Verify order roughly
        all_ids = []
        for b in batches:
            all_ids.extend(b["record_ids"])
            
        assert "rec_1" in all_ids
        assert "rec_5" in all_ids
        assert len(set(all_ids)) == 5
        
    print("Integration test passed successfully!")


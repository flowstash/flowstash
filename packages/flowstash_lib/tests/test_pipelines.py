import pytest
import asyncio
import json
from datetime import datetime, UTC
from unittest.mock import AsyncMock, patch, MagicMock
from flowstash.pipelines.records_model import RecordData
from flowstash.pipelines.records_feed import RecordsFeed
from flowstash.pipelines.consumer import feed_consumer, ConsumerRunner, _consumers
from flowstash.context import integration_context

@pytest.fixture(autouse=True)
def clear_consumers():
    import flowstash.pipelines.records_feed as records_feed
    records_feed._sha = None
    _consumers.clear()
    yield
    _consumers.clear()

@pytest.mark.asyncio
async def test_records_feed_publish_latest_wins():
    mock_redis = AsyncMock()
    mock_redis.script_load.return_value = "cached_sha"
    mock_redis.evalsha.side_effect = [1, 0] 
    
    with patch("flowstash.pipelines.records_feed.get_redis_client", return_value=mock_redis):
        with integration_context(integration="test", tenant_id="t1"):
            feed = RecordsFeed.get("test_feed")
            
            # 1. First publish
            rec1 = RecordData(record_id="r1", record_type="type", data={"v": 1}, timestamp=datetime(2024, 1, 1, tzinfo=UTC))
            await feed.publish(rec1)
            
            assert mock_redis.script_load.called
            assert mock_redis.evalsha.called
            
            # 2. Second publish
            rec2 = RecordData(record_id="r1", record_type="type", data={"v": 0}, timestamp=datetime(2023, 1, 1, tzinfo=UTC))
            await feed.publish(rec2)
            
            assert mock_redis.evalsha.call_count == 2
            # Verify args passed: sha, num_keys, k1, k2, k3, dedupe_key, ts_val, payload
            # (Note: evalsha(sha, num_keys, *keys_and_args))
            args = mock_redis.evalsha.call_args_list[0][0]
            assert args[1] == 3
            assert args[3] == f"rf:{feed.feed_id}:ts"


@pytest.mark.asyncio
async def test_records_feed_publish_missing_timestamp():
    mock_redis = AsyncMock()
    mock_redis.script_load.return_value = "sha"
    mock_redis.evalsha.return_value = 1
    
    with patch("flowstash.pipelines.records_feed.get_redis_client", return_value=mock_redis):
        with integration_context(integration="test", tenant_id="t1"):
            feed = RecordsFeed.get("test_feed")
            
            rec = RecordData(record_id="r1", record_type="type", data={"v": 1})
            await feed.publish(rec)
            
            # args = [dedupe_key, ts_val, payload]
            args = mock_redis.evalsha.call_args[0][5:]
            ts_sent = float(args[1])
            assert ts_sent > 0 


@pytest.mark.asyncio
async def test_consumer_registration():
    @feed_consumer(feed_id="test_feed", subscription="sub1")
    async def my_handler(records):
        pass
    
    assert len(_consumers) == 1
    assert _consumers[0].feed_id == "test_feed"
    assert _consumers[0].subscription_name == "sub1"

@pytest.mark.asyncio
async def test_consumer_runner_loop():
    mock_redis = AsyncMock()
    
    # Mock data from stream
    # stream_batch = [(id, {dedupe_key: ...})]
    mock_redis.xreadgroup.side_effect = [
        [["rf:test_feed:stream", [("1-0", {"dedupe_key": "test:type:r1"})]]],
        None # Stop after one read
    ]
    
    # Mock data from latest hash
    record_json = json.dumps({
        "record_id": "r1",
        "record_type": "type",
        "data": {"foo": "bar"},
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
        "dedupe_key": "test:type:r1",
        "_ts_val": 1704067200.0
    })
    
    # pipeline() is NOT a coroutine in redis-py, but execute() IS.
    pipeline_mock = MagicMock()
    pipeline_mock.hget = MagicMock()
    pipeline_mock.execute = AsyncMock(return_value=[record_json])
    mock_redis.pipeline = MagicMock(return_value=pipeline_mock)


    
    handler_called = asyncio.Event()
    received_records = []

    @feed_consumer(feed_id="test_feed", subscription="sub1")
    async def my_handler(records):
        received_records.append(records)
        handler_called.set()

    runner = ConsumerRunner(tenant_id="test_tenant")
    
    # Mock AsyncManager to just call the function directly
    mock_async_mgr = MagicMock()
    mock_async_mgr.execute = AsyncMock(side_effect=lambda f, *a: f(*a))
    
    with patch("flowstash.pipelines.consumer.get_redis_client", return_value=mock_redis), \
         patch("flowstash.observability.ingestion.AsyncManager.get_instance", return_value=mock_async_mgr):
        # We need to start the runner and wait for the handler to be called
        await runner.start()
        try:
            await asyncio.wait_for(handler_called.wait(), timeout=2)
        finally:
            await runner.stop()

            
    assert len(received_records) == 1
    assert received_records[0].record_id == "r1"
    assert received_records[0].data["foo"] == "bar"
    
    # Verify ACK was called
    mock_redis.xack.assert_called_with("rf:test_feed:stream", "sub1", "1-0")

@pytest.mark.asyncio
async def test_consumer_batching_accumulation():
    mock_redis = AsyncMock()
    
    # First read returns 1 item
    # Second read (hold-off) returns 1 more item
    mock_redis.xreadgroup.side_effect = [
        [["rf:test_feed:stream", [("1-0", {"dedupe_key": "k1"})]]],
        [["rf:test_feed:stream", [("2-0", {"dedupe_key": "k2"})]]],
        None # Then nothing
    ]
    
    # Hash snapshots
    r1_json = json.dumps({"record_id": "r1", "record_type": "t", "data": {}, "timestamp": None, "dedupe_key": "k1", "_ts_val": 0})
    r2_json = json.dumps({"record_id": "r2", "record_type": "t", "data": {}, "timestamp": None, "dedupe_key": "k2", "_ts_val": 0})
    
    pipeline_mock = MagicMock()
    pipeline_mock.hget = MagicMock()
    pipeline_mock.execute = AsyncMock(return_value=[r1_json, r2_json])
    mock_redis.pipeline = MagicMock(return_value=pipeline_mock)


    
    handler_called = asyncio.Event()
    received_batch = []

    @feed_consumer(feed_id="test_feed", batch=True, max_batch_size=10, max_delay_ms=200)
    async def batch_handler(records):
        received_batch.extend(records)
        handler_called.set()

    runner = ConsumerRunner(tenant_id="test_tenant")
    
    # Mock AsyncManager
    mock_async_mgr = MagicMock()
    mock_async_mgr.execute = AsyncMock(side_effect=lambda f, *a: f(*a))

    with patch("flowstash.pipelines.consumer.get_redis_client", return_value=mock_redis), \
         patch("flowstash.observability.ingestion.AsyncManager.get_instance", return_value=mock_async_mgr):
        await runner.start()
        try:
            await asyncio.wait_for(handler_called.wait(), timeout=2)
        finally:
            await runner.stop()

            
    assert len(received_batch) == 2
    assert received_batch[0].record_id == "r1"
    assert received_batch[1].record_id == "r2"

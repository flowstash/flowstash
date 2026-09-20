import json
import asyncio
import logging
import functools
import inspect
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Callable, List, Union, Optional, Dict
from .records_model import RecordData
from .record_serialization import from_jsonable
from .redis_client import get_redis_client
from ..context import integration_context, current_context
from ..observability.ingestion import enqueue_record_link
from ..observability.model import RecordLinkKind


logger = logging.getLogger(__name__)

@dataclass
class ConsumerSpec:
    handler: Callable
    feed_id: str
    subscription_name: str
    batch: bool
    max_batch_size: int
    max_delay_ms: int
    rate_limit_per_sec: Optional[int]
    concurrency: Optional[int]
    schedule: Optional[str] = None
    debounce_delay_ms: int = 0  # 0 = disabled; classic (non-batched) consumers only
    max_debounce_window_ms: int = 0  # 0 = default cap; sliding-window upper bound
    # None = no opinion, platform default applies; 0 = explicitly disabled.
    dedupe_window_ms: Optional[int] = None

_consumers: List[ConsumerSpec] = []

def get_registered_consumers() -> List[ConsumerSpec]:
    return _consumers


def feed_consumer(
    feed_id: str,
    subscription: Optional[str] = None,
    batch: bool = False,
    max_batch_size: int = 100,
    max_delay_ms: int = 500,
    rate_limit_per_sec: Optional[int] = None,
    concurrency: Optional[int] = None,
    schedule: Optional[str] = None,
    debounce_delay_ms: int = 0,
    max_debounce_window_ms: int = 0,
    dedupe_window_ms: Optional[int] = None,
):

    """
    Decorator for record consumers.

    debounce_delay_ms: when > 0 (classic/non-batched consumers only), collapses a
        burst of events sharing a dedupe_key into a single delivery of the latest
        payload, fired once arrivals quiet down for this many ms (sliding window).
    max_debounce_window_ms: upper bound on how long a key can be debounced before
        it is forcibly delivered (0 = use platform default cap).
    dedupe_window_ms: for classic consumers with no debounce, drop any record whose
        dedupe_key was already delivered within this many ms. Unlike debounce this
        delivers the FIRST record immediately and discards the rest permanently —
        they are not buffered and cannot be recovered. None uses the platform
        default (5 minutes); set 0 to opt out and receive every publish.
    """
    if debounce_delay_ms and batch:
        raise ValueError(
            "debounce_delay_ms is only supported on classic (non-batched) consumers"
        )
    if dedupe_window_ms:
        if batch:
            raise ValueError(
                "dedupe_window_ms is only supported on classic (non-batched) consumers"
            )
        if debounce_delay_ms:
            raise ValueError(
                "dedupe_window_ms cannot be combined with debounce_delay_ms; "
                "debounce already collapses same-key bursts"
            )
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(records: Union[RecordData, List[RecordData]], *args, **kwargs):
            # Resolve integration/pipeline/tenant from context 
            # (assuming this is called by a worker that set up basic context)
            parent = current_context()
            
            # For each record, emit RecordLink (CONSUMED)
            records_list = records if isinstance(records, list) else [records]
            
            # Use integration/pipeline from records or context
            # (RecordData doesn't have it, but RecordsFeed.publish uses context)
            integration = parent.integration if parent else "unknown"
            pipeline = parent.integration_pipeline if parent else "unknown"
            
            # Emit consumption links
            for r in records_list:
                dedupe_key = r.get_dedupe_key(integration)
                if parent:
                    await enqueue_record_link(
                        tenant_id=parent.tenant_id,
                        integration=integration,
                        pipeline=pipeline,
                        run_id=parent.run_id,
                        trace_id=parent.trace_id,
                        span_id=parent.span_id,
                        record_key=dedupe_key,
                        kind=RecordLinkKind.CONSUMED,
                        source=feed_id
                    )


            # Set "current record scope"
            # If batch=True, maybe we can't set a single record_key, or we pick the first one?
            # Or we leave it None.
            scope_key = records_list[0].get_dedupe_key(integration) if not batch and records_list else None
            
            with integration_context(current_record_key=scope_key):
                # Execute handler
                if inspect.iscoroutinefunction(func):
                    return await func(records, *args, **kwargs)
                else:
                    return func(records, *args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(records: Union[RecordData, List[RecordData]], *args, **kwargs):
            parent = current_context()
            records_list = records if isinstance(records, list) else [records]
            integration = parent.integration if parent else "unknown"
            pipeline = parent.integration_pipeline if parent else "unknown"
            
            for r in records_list:
                dedupe_key = r.get_dedupe_key(integration)
                if parent:
                    # In sync code, we fire and forget
                    import asyncio
                    coro = enqueue_record_link(
                        tenant_id=parent.tenant_id,
                        integration=integration,
                        pipeline=pipeline,
                        run_id=parent.run_id,
                        trace_id=parent.trace_id,
                        span_id=parent.span_id,
                        record_key=dedupe_key,
                        kind=RecordLinkKind.CONSUMED,
                        source=feed_id
                    )
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(coro)
                        else:
                            loop.run_until_complete(coro)
                    except RuntimeError:
                        asyncio.run(coro)

            scope_key = records_list[0].get_dedupe_key(integration) if not batch and records_list else None
            with integration_context(current_record_key=scope_key):
                return func(records, *args, **kwargs)

        target_wrapper = async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
        
        # Determine subscription name
        sub_name = subscription or f"{func.__module__}.{func.__qualname__}"
        
        # Register consumer
        _consumers.append(ConsumerSpec(
            handler=target_wrapper,
            feed_id=feed_id,
            subscription_name=sub_name,
            batch=batch,
            max_batch_size=max_batch_size,
            max_delay_ms=max_delay_ms,
            rate_limit_per_sec=rate_limit_per_sec,
            concurrency=concurrency,
            schedule=schedule,
            debounce_delay_ms=debounce_delay_ms,
            max_debounce_window_ms=max_debounce_window_ms,
            dedupe_window_ms=dedupe_window_ms,
        ))
        
        return target_wrapper
    return decorator

class ConsumerRunner:
    """
    Runtime for processing Record Feeds.
    """
    def __init__(self, tenant_id: str = "default"):
        self.tenant_id = tenant_id
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def start(self):
        self._running = True
        redis_client = get_redis_client()
        
        for spec in _consumers:
            if spec.schedule:
                # Scheduled consumption is handled by a separate cadence
                # For now we skip or implement minimal loop
                self._tasks.append(asyncio.create_task(self._scheduled_loop(spec)))
            else:
                self._tasks.append(asyncio.create_task(self._consumer_loop(spec)))

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _consumer_loop(self, spec: ConsumerSpec):
        redis_client = get_redis_client()
        stream_key = f"rf:{spec.feed_id}:stream"
        latest_key = f"rf:{spec.feed_id}:latest"
        group_name = spec.subscription_name
        
        # Ensure group exists
        try:
            await redis_client.xgroup_create(stream_key, group_name, id="0", mkstream=True)
        except Exception:
            pass

        consumer_name = f"worker-{asyncio.get_event_loop().time()}"

        while self._running:
            try:
                # 1. Read from stream
                # If batching, we might want to wait a bit to accumulate
                entries = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=spec.max_batch_size if spec.batch else 1,
                    block=1000
                )
                
                if not entries:
                    continue

                stream_batch = entries[0][1]
                
                # Hold-off window implementation
                if spec.batch and len(stream_batch) < spec.max_batch_size:
                    start_time = asyncio.get_event_loop().time()
                    while (asyncio.get_event_loop().time() - start_time) < (spec.max_delay_ms / 1000.0):
                        wait_remaining = int(spec.max_delay_ms - (asyncio.get_event_loop().time() - start_time) * 1000)
                        if wait_remaining <= 0: break
                        
                        more_entries = await redis_client.xreadgroup(
                            groupname=group_name,
                            consumername=consumer_name,
                            streams={stream_key: ">"},
                            count=spec.max_batch_size - len(stream_batch),
                            block=wait_remaining
                        )
                        if more_entries:
                            stream_batch.extend(more_entries[0][1])
                            if len(stream_batch) >= spec.max_batch_size:
                                break
                        else:
                            break # timeout or nothing else

                # 2. Collect dedupe keys and ids
                batch_data: Dict[str, str] = {} # dedupe_key -> stream_id
                for entry_id, entry_fields in stream_batch:
                    dk = entry_fields.get("dedupe_key")
                    if dk:
                        batch_data[dk] = entry_id

                # 3. Fetch latest snapshots
                pipe = redis_client.pipeline()
                dks = list(batch_data.keys())
                for dk in dks:
                    pipe.hget(latest_key, dk)
                
                snapshots = await pipe.execute()
                
                from ..observability.registry import get_blob_store

                async def _load_record(snap: str):
                    try:
                        snap_data = json.loads(snap)
                        record_data = snap_data.get("data")
                        
                        if snap_data.get("blob_ref"):
                            try:
                                # Fetch blob using executor
                                blob_bytes = await asyncio.to_thread(
                                    get_blob_store().get, snap_data["blob_ref"]
                                )
                                record_data = json.loads(blob_bytes)
                            except Exception as e:
                                logger.error(f"Failed to fetch blob {snap_data['blob_ref']}: {e}")
                                return None

                        return RecordData(
                            record_id=snap_data["record_id"],
                            record_type=snap_data["record_type"],
                            data=from_jsonable(record_data),
                            timestamp=datetime.fromisoformat(snap_data["timestamp"]) if snap_data["timestamp"] else None,
                            dedupe_key=snap_data.get("dedupe_key"),
                            source_integration=snap_data.get("source_integration"),
                            source_pipeline=snap_data.get("source_pipeline"),
                            source_run_id=snap_data.get("source_run_id"),
                            source_traceparent=snap_data.get("source_traceparent"),
                        )
                    except Exception as e:
                        logger.error(f"Error parsing record snapshot: {e}")
                        return None

                # Load records in parallel (especially if blobs are involved)
                load_tasks = [
                    _load_record(snap) 
                    for snap in snapshots if snap
                ]
                if load_tasks:
                    records = [r for r in await asyncio.gather(*load_tasks) if r]
                else:
                    records = []

                if not records:
                    ids_to_ack = [entry_id for entry_id, _ in stream_batch]
                    await redis_client.xack(stream_key, group_name, *ids_to_ack)
                    continue


                # 4. Deliver
                try:
                    first = records[0] if records else None
                    with integration_context(
                        integration=first.source_integration if first and first.source_integration else spec.feed_id,
                        integration_pipeline=first.source_pipeline if first else None,
                        tenant_id=self.tenant_id
                    ) as ctx:
                        if spec.batch:
                            await spec.handler(records)
                        else:
                            for r in records:
                                await spec.handler(r)
                    
                    # 5. ACK
                    ids_to_ack = [entry_id for entry_id, _ in stream_batch]
                    await redis_client.xack(stream_key, group_name, *ids_to_ack)
                except Exception as e:
                    logger.error(f"Error in consumer handler: {e}", exc_info=True)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                await asyncio.sleep(1)


    async def _scheduled_loop(self, spec: ConsumerSpec):
        # Simplistic "check once a minute" drain for illustration
        while self._running:
            # In a real system, we'd use a real scheduler.
            # For this task, we can just drain once and wait or skip implementation.
            await asyncio.sleep(60)


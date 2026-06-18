import json
import asyncio
from datetime import datetime, UTC
from typing import Optional, Any, Protocol, runtime_checkable

from pydantic import BaseModel
from .records_model import RecordData
from .record_serialization import to_jsonable
from .redis_client import get_redis_client
from ..context import get_context, integration_context
from ..observability.ingestion import enqueue_record_link
from ..observability.model import RecordLinkKind
from ..observability.registry import get_blob_store

_PUBLISH_SCRIPT = """
local latest_key = KEYS[1]
local ts_key = KEYS[2]
local stream_key = KEYS[3]
local dedupe_key = ARGV[1]
local new_ts = tonumber(ARGV[2])
local data = ARGV[3]

local old_ts = tonumber(redis.call('HGET', ts_key, dedupe_key) or 0)

if new_ts > old_ts then
    redis.call('HSET', latest_key, dedupe_key, data)
    redis.call('HSET', ts_key, dedupe_key, ARGV[2])
    redis.call('XADD', stream_key, '*', 'dedupe_key', dedupe_key, 'ts', ARGV[2])
    return 1
end
return 0
"""
_sha = None


@runtime_checkable
class FeedBackend(Protocol):
    async def publish(self, feed_id: str, record: RecordData) -> str: ...


_feed_backend: Optional[FeedBackend] = None


def set_feed_backend(backend: FeedBackend) -> FeedBackend:
    global _feed_backend
    _feed_backend = backend
    return backend


def get_feed_backend() -> Optional[FeedBackend]:
    return _feed_backend


class RecordsFeed:
    def __init__(self, feed_id: str):
        self.feed_id = feed_id

    @classmethod
    def get(cls, feed_id: str) -> "RecordsFeed":
        return cls(feed_id)

    async def _execute_script(self, redis_client, keys, args):
        global _sha
        if _sha is None:
            _sha = await redis_client.script_load(_PUBLISH_SCRIPT)

        try:
            return await redis_client.evalsha(_sha, len(keys), *keys, *args)
        except Exception as e:
            if "NOSCRIPT" in str(e):
                _sha = await redis_client.script_load(_PUBLISH_SCRIPT)
                return await redis_client.evalsha(_sha, len(keys), *keys, *args)
            raise

    async def publish(self, record: RecordData):
        ctx = get_context()
        if not ctx:
            with integration_context(record_lifecycle=False) as ctx:
                await self.publish(record)
            return

        backend = get_feed_backend()
        if backend:
            await backend.publish(self.feed_id, record)
            return

        integration = ctx.integration or "unknown"
        dedupe_key = record.get_dedupe_key(integration)

        # Policy A: Use now() if timestamp is missing
        effective_ts = record.timestamp or datetime.now(UTC)
        ts_val = effective_ts.timestamp()

        # Externalize large data to BlobStore (> 5KB)
        data_to_store = to_jsonable(record.data)
        blob_ref = None

        raw_data = json.dumps(data_to_store).encode("utf-8")
        if len(raw_data) > 5120:
            try:
                # Use a specific path in blob store for feed records
                # Prefix with isoDate for organization if needed, but registry handles that usually
                blob_path = f"feeds/{self.feed_id}/{record.record_type}/{dedupe_key}"
                blob_ref, _, _ = get_blob_store().put(
                    path_hint=blob_path, content_type="application/json", data=raw_data
                )
                data_to_store = None  # Only store reference
            except Exception:
                # Fallback to Redis if blob store is unavailable
                pass

        # 1. Prepare payload for Redis HASH
        payload = {
            "record_id": record.record_id,
            "record_type": record.record_type,
            "data": data_to_store,
            "blob_ref": blob_ref,
            "timestamp": effective_ts.isoformat(),
            "dedupe_key": dedupe_key,
            "source_integration": ctx.integration,
            "source_pipeline": ctx.integration_pipeline,
            "source_run_id": ctx.run_id,
            "source_traceparent": ctx.traceparent,
        }
        payload_json = json.dumps(payload)

        # 2. Atomic Publish
        redis_client = get_redis_client()
        keys = [
            f"rf:{self.feed_id}:latest",
            f"rf:{self.feed_id}:ts",
            f"rf:{self.feed_id}:stream",
        ]
        args = [dedupe_key, str(ts_val), payload_json]

        args = [dedupe_key, str(ts_val), payload_json]

        await self._execute_script(redis_client, keys, args)

        # 3. Emit RecordLink (PUBLISHED)
        await enqueue_record_link(
            tenant_id=ctx.tenant_id,
            integration=integration,
            pipeline=ctx.integration_pipeline,
            run_id=ctx.run_id,
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            record_key=dedupe_key,
            kind=RecordLinkKind.PUBLISHED,
            source=self.feed_id,
        )

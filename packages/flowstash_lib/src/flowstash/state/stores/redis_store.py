from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any, Optional

from ..entry import StateEntry
from ..protocol import _validate, _decode_entry


def _b64(s: str) -> str:
    """Base64url-encode a string to produce a safe Redis key component."""
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _epoch_to_dt(epoch: Optional[int]) -> Optional[datetime]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class RedisStateStore:
    """
    Redis-backed StateStore (async redis via redis-py).

    Key layout: ``state:{b64(namespace)}:{b64(key)}``

    Each entry is stored as a Redis HASH with fields:
      value, content_type, updated_at, expires_at, version

    TTL is managed at the Redis key level:
      - ttl_s provided → EXPIRE key ttl_s
      - ttl_s=None     → PERSIST key  (removes any prior TTL)
    """

    KEY_PREFIX = "state"

    def __init__(self, redis_client=None, redis_url: Optional[str] = None):
        """
        Provide either an already-created redis client, or a redis_url.
        If neither is given, the shared get_redis_client() helper is used.
        """
        self._client = redis_client
        self._redis_url = redis_url

    def _get_client(self):
        if self._client is not None:
            return self._client
        from flowstash.pipelines.redis_client import get_redis_client

        return get_redis_client(self._redis_url)

    def _redis_key(self, namespace: str, key: str) -> str:
        return f"{self.KEY_PREFIX}:{_b64(namespace)}:{_b64(key)}"

    # ------------------------------------------------------------------
    # Protocol implementation (sync wrappers — async callers should use
    # the async variants prefixed with a_)
    # ------------------------------------------------------------------

    def _run_async(self, coro):
        """
        Run an async coroutine from either a sync or async calling context.

        When called from within a running event loop (e.g. inside an asyncio.Task
        spawned by AsyncioBackend), run_until_complete() would raise
        'This event loop is already running'. We detect this and offload to a
        background thread with its own event loop instead.
        """
        import asyncio
        import concurrent.futures

        try:
            asyncio.get_running_loop()
            # Running inside an event loop — delegate to a thread.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result()
        except RuntimeError:
            # No running loop — safe to start one.
            return asyncio.run(coro)

    def get_entry(self, namespace: str, key: str) -> Optional[StateEntry]:
        return self._run_async(self.aget_entry(namespace, key))

    def get(self, namespace: str, key: str) -> Optional[Any]:
        return self._run_async(self.aget(namespace, key))

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        content_type: str = "application/json",
        ttl_s: Optional[int] = None,
    ) -> StateEntry:
        return self._run_async(
            self.aset(namespace, key, value, content_type=content_type, ttl_s=ttl_s)
        )

    # ------------------------------------------------------------------
    # Async variants
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_hash(data: dict) -> dict:
        """Normalise hgetall output to {str: str|bytes} regardless of decode_responses setting."""
        result = {}
        for k, v in data.items():
            k_str = k.decode("utf-8") if isinstance(k, bytes) else k
            result[k_str] = v
        return result

    @staticmethod
    def _decode_field(v) -> str:
        """Decode a hash field value to str."""
        return v.decode("utf-8") if isinstance(v, bytes) else str(v)

    async def aget_entry(self, namespace: str, key: str) -> Optional[StateEntry]:
        _validate(namespace, key)
        client = self._get_client()
        rkey = self._redis_key(namespace, key)
        raw_data = await client.hgetall(rkey)
        if not raw_data:
            return None

        data = self._normalise_hash(raw_data)

        expires_at_epoch: Optional[int] = None
        exp_raw = data.get("expires_at", "")
        exp_str = self._decode_field(exp_raw) if exp_raw else ""
        if exp_str:
            expires_at_epoch = int(exp_str)

        now = _now_epoch()
        if expires_at_epoch is not None and expires_at_epoch <= now:
            await client.delete(rkey)
            return None

        # Value is stored as base64 ASCII
        val_raw = data["value"]
        val_b64 = val_raw if isinstance(val_raw, str) else val_raw.decode("ascii")
        raw_value = base64.b64decode(val_b64)

        content_type = self._decode_field(data.get("content_type", "application/json"))
        updated_at_epoch = int(self._decode_field(data.get("updated_at", 0)))
        version = int(self._decode_field(data.get("version", 1)))

        return StateEntry(
            value=raw_value,
            content_type=content_type,
            updated_at=_epoch_to_dt(updated_at_epoch),
            expires_at=_epoch_to_dt(expires_at_epoch),
            version=version,
        )

    async def aget(self, namespace: str, key: str) -> Optional[Any]:
        entry = await self.aget_entry(namespace, key)
        if entry is None:
            return None
        return _decode_entry(entry)

    async def aset(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        content_type: str = "application/json",
        ttl_s: Optional[int] = None,
    ) -> StateEntry:
        _validate(namespace, key)
        client = self._get_client()
        rkey = self._redis_key(namespace, key)
        now = _now_epoch()
        expires_at: Optional[int] = now + ttl_s if ttl_s is not None else None

        # Encode value
        if content_type == "application/json":
            if isinstance(value, bytes):
                raw = value
            else:
                raw = json.dumps(value).encode("utf-8")
        elif isinstance(value, bytes):
            raw = value
        elif isinstance(value, str):
            raw = value.encode("utf-8")
        else:
            raw = str(value).encode("utf-8")

        # Fetch current version to bump it
        existing_version_raw = await client.hget(rkey, "version")
        if existing_version_raw:
            version = int(self._decode_field(existing_version_raw)) + 1
        else:
            version = 1

        await client.hset(
            rkey,
            mapping={
                "value": base64.b64encode(raw).decode("ascii"),
                "content_type": content_type,
                "updated_at": now,
                "expires_at": expires_at if expires_at is not None else "",
                "version": version,
            },
        )

        if ttl_s is not None:
            await client.expire(rkey, ttl_s)
        else:
            await client.persist(rkey)

        return StateEntry(
            value=raw,
            content_type=content_type,
            updated_at=_epoch_to_dt(now),
            expires_at=_epoch_to_dt(expires_at),
            version=version,
        )

"""
Acceptance tests for StateStore implementations.

SQLiteStateStore: in-memory :memory: instance (no file side-effects).
RedisStateStore:  real Redis at REDIS_URL (default localhost:6379).
                  Keys are prefixed with a unique run-ID and cleaned up after each test.
"""
from __future__ import annotations

import uuid
import pytest
import pytest_asyncio

from flowstash.state.stores.sqlite_store import SQLiteStateStore
from flowstash.state.stores.redis_store import RedisStateStore
from flowstash.state.entry import StateEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sqlite_store():
    """Fresh in-memory SQLite store per test."""
    return SQLiteStateStore(db_path=":memory:")


@pytest_asyncio.fixture()
async def redis_store():
    """
    RedisStateStore backed by the real Redis instance.
    Uses a unique key prefix per test to avoid cross-test collisions,
    and cleans up all keys it created after each test.
    """
    import redis.asyncio as aioredis
    import os

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    client = aioredis.from_url(redis_url, decode_responses=False)

    # Verify connection
    await client.ping()

    # Use test-scoped prefix so we can clean up afterwards
    run_id = uuid.uuid4().hex[:8]
    store = RedisStateStore(redis_client=client)
    # Override prefix so test keys are easily identifiable
    store.KEY_PREFIX = f"test:{run_id}:state"

    yield store

    # Cleanup: delete all keys created by this test
    pattern = f"test:{run_id}:state:*"
    keys = await client.keys(pattern)
    if keys:
        await client.delete(*keys)
    await client.aclose()


# ---------------------------------------------------------------------------
# 1. Roundtrip
# ---------------------------------------------------------------------------

class TestRoundtrip:
    def test_sqlite_roundtrip(self, sqlite_store):
        sqlite_store.set("ns1", "k1", {"hello": "world"})
        assert sqlite_store.get("ns1", "k1") == {"hello": "world"}

    def test_sqlite_roundtrip_string(self, sqlite_store):
        sqlite_store.set("ns1", "k2", "simple string", content_type="text/plain")
        assert sqlite_store.get("ns1", "k2") == "simple string"

    @pytest.mark.asyncio
    async def test_redis_roundtrip(self, redis_store):
        await redis_store.aset("ns1", "k1", {"hello": "redis"})
        result = await redis_store.aget("ns1", "k1")
        assert result == {"hello": "redis"}


# ---------------------------------------------------------------------------
# 2. TTL expiry
# ---------------------------------------------------------------------------

class TestTTLExpiry:
    def test_sqlite_expired_returns_none(self, sqlite_store):
        from flowstash.state.stores.sqlite_store import _now_epoch

        conn = sqlite_store._conn()
        past = _now_epoch() - 10
        conn.execute(
            "INSERT INTO state_entries (namespace, key, value, content_type, updated_at, expires_at, version) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            ("ns", "expired_key", b'{"x":1}', "application/json", past - 100, past),
        )
        conn.commit()
        assert sqlite_store.get("ns", "expired_key") is None

    @pytest.mark.asyncio
    async def test_redis_expired_returns_none(self, redis_store):
        """Set a key, then immediately expire it at the Redis level → get should return None."""
        await redis_store.aset("ns", "expkey", {"v": 1}, ttl_s=60)

        # Force-expire the key in Redis right now
        rkey = redis_store._redis_key("ns", "expkey")
        client = redis_store._get_client()
        await client.expire(rkey, 0)  # 0 seconds = immediate expiry

        result = await redis_store.aget("ns", "expkey")
        assert result is None


# ---------------------------------------------------------------------------
# 3. TTL removal (set ttl → overwrite with no ttl → key persists)
# ---------------------------------------------------------------------------

class TestTTLRemoval:
    def test_sqlite_ttl_removal(self, sqlite_store):
        sqlite_store.set("ns", "k", {"v": 1}, ttl_s=1)
        sqlite_store.set("ns", "k", {"v": 2})  # overwrite, no TTL

        conn = sqlite_store._conn()
        row = conn.execute(
            "SELECT expires_at FROM state_entries WHERE namespace='ns' AND key='k'"
        ).fetchone()
        assert row["expires_at"] is None
        assert sqlite_store.get("ns", "k") == {"v": 2}

    @pytest.mark.asyncio
    async def test_redis_ttl_removal(self, redis_store):
        await redis_store.aset("ns", "k", {"v": 1}, ttl_s=60)
        await redis_store.aset("ns", "k", {"v": 2})  # no TTL → PERSIST called

        client = redis_store._get_client()
        rkey = redis_store._redis_key("ns", "k")
        ttl = await client.ttl(rkey)
        assert ttl == -1, f"expected no expiry (TTL=-1), got {ttl}"

        assert await redis_store.aget("ns", "k") == {"v": 2}


# ---------------------------------------------------------------------------
# 4. Overwrite / version bump
# ---------------------------------------------------------------------------

class TestOverwrite:
    def test_sqlite_version_bumps_on_overwrite(self, sqlite_store):
        e1 = sqlite_store.set("ns", "k", {"v": 1})
        e2 = sqlite_store.set("ns", "k", {"v": 2})
        assert e2.version == e1.version + 1
        assert sqlite_store.get("ns", "k") == {"v": 2}

    @pytest.mark.asyncio
    async def test_redis_version_bumps_on_overwrite(self, redis_store):
        e1 = await redis_store.aset("ns", "k", {"v": 1})
        e2 = await redis_store.aset("ns", "k", {"v": 2})
        assert e2.version == e1.version + 1
        assert await redis_store.aget("ns", "k") == {"v": 2}


# ---------------------------------------------------------------------------
# 5. Namespace isolation
# ---------------------------------------------------------------------------

class TestNamespaceIsolation:
    def test_sqlite_namespace_isolation(self, sqlite_store):
        sqlite_store.set("ns_a", "same_key", {"owner": "a"})
        sqlite_store.set("ns_b", "same_key", {"owner": "b"})
        assert sqlite_store.get("ns_a", "same_key") == {"owner": "a"}
        assert sqlite_store.get("ns_b", "same_key") == {"owner": "b"}

    @pytest.mark.asyncio
    async def test_redis_namespace_isolation(self, redis_store):
        await redis_store.aset("ns_a", "same_key", {"owner": "a"})
        await redis_store.aset("ns_b", "same_key", {"owner": "b"})
        assert await redis_store.aget("ns_a", "same_key") == {"owner": "a"}
        assert await redis_store.aget("ns_b", "same_key") == {"owner": "b"}


# ---------------------------------------------------------------------------
# 6. Validation — empty namespace/key
# ---------------------------------------------------------------------------

class TestValidation:
    def test_sqlite_empty_namespace_raises(self, sqlite_store):
        with pytest.raises(ValueError, match="namespace"):
            sqlite_store.set("", "k", {})

    def test_sqlite_empty_key_raises(self, sqlite_store):
        with pytest.raises(ValueError, match="key"):
            sqlite_store.set("ns", "", {})

    def test_sqlite_get_missing_returns_none(self, sqlite_store):
        assert sqlite_store.get("ns", "nonexistent") is None

    @pytest.mark.asyncio
    async def test_redis_empty_namespace_raises(self, redis_store):
        with pytest.raises(ValueError, match="namespace"):
            await redis_store.aset("", "k", {})

    @pytest.mark.asyncio
    async def test_redis_empty_key_raises(self, redis_store):
        with pytest.raises(ValueError, match="key"):
            await redis_store.aset("ns", "", {})

    @pytest.mark.asyncio
    async def test_redis_get_missing_returns_none(self, redis_store):
        assert await redis_store.aget("ns", "definitely_not_there") is None


# ---------------------------------------------------------------------------
# 7. SQLiteStateStore.purge_expired
# ---------------------------------------------------------------------------

class TestPurgeExpired:
    def test_purge_expired_removes_rows(self, sqlite_store):
        from flowstash.state.stores.sqlite_store import _now_epoch

        conn = sqlite_store._conn()
        past = _now_epoch() - 10
        conn.execute(
            "INSERT INTO state_entries (namespace, key, value, content_type, updated_at, expires_at, version) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            ("ns", "dead_key", b'{}', "application/json", past - 100, past),
        )
        conn.commit()
        deleted = sqlite_store.purge_expired()
        assert deleted >= 1
        assert sqlite_store.get("ns", "dead_key") is None

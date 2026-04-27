import redis.asyncio as redis
from typing import Optional
import asyncio
import weakref
import os

_redis_clients = weakref.WeakKeyDictionary()
_global_fallback_client: Optional[redis.Redis] = None

def get_redis_client(redis_url: Optional[str] = None) -> redis.Redis:
    """
    Get a shared async Redis client.
    If redis_url is provided, it will initialize/re-initialize the client.
    The client is bound to the current running asyncio loop because
    Redis async clients are not easily shared across different loops.
    """
    global _global_fallback_client

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if redis_url:
        client = redis.from_url(redis_url, decode_responses=True)
        if loop is not None:
            _redis_clients[loop] = client
        else:
            _global_fallback_client = client
        return client

    if loop is not None and loop in _redis_clients:
        return _redis_clients[loop]

    if loop is None and _global_fallback_client is not None:
        return _global_fallback_client

    env_redis_url = os.getenv("REDIS_URL")
    if env_redis_url:
        print(f"Connecting to Redis at {env_redis_url}")
        client = redis.from_url(env_redis_url, decode_responses=True)
    else:
        # Try to initialize with defaults if not already done
        print("Connecting to Redis at localhost:6379")
        client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    if loop is not None:
        _redis_clients[loop] = client
    else:
        _global_fallback_client = client

    return client

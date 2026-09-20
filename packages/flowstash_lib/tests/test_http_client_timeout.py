import asyncio
import contextlib

import httpx
import pytest
from unittest.mock import AsyncMock, patch

from flowstash.clients.config import ClientSettings, RetryConfig
from flowstash.clients.http import HttpClient


@contextlib.asynccontextmanager
async def blackhole_server():
    """A real TCP server that accepts connections and never replies."""
    stop = asyncio.Event()

    async def _handle(reader, writer):
        try:
            await stop.wait()
        finally:
            writer.close()

    server = await asyncio.start_server(_handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        stop.set()
        server.close()
        await server.wait_closed()


def _client(port: int, timeout: float) -> HttpClient:
    settings = ClientSettings(
        client_id="timeout_api",
        base_url=f"http://127.0.0.1:{port}",
        timeout=timeout,
        retry=RetryConfig(max_retries=0),
    )
    return HttpClient(name="TIMEOUT", settings=settings)


@pytest.mark.asyncio
async def test_client_timeout_applies_when_no_per_request_timeout():
    """settings.timeout must apply to requests that pass no timeout of their own.

    Regression: `timeout=None` was forwarded to httpx, which reads it as
    "no timeout at all" instead of "fall back to the client default", so every
    request hung forever against a server that never replied.
    """
    async with blackhole_server() as port:
        client = _client(port, timeout=0.3)
        with patch(
            "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
        ):
            with pytest.raises(httpx.ReadTimeout):
                await asyncio.wait_for(client.request("GET", "/hang"), timeout=5)


@pytest.mark.asyncio
async def test_per_request_timeout_overrides_client_timeout():
    async with blackhole_server() as port:
        client = _client(port, timeout=30.0)
        with patch(
            "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
        ):
            with pytest.raises(httpx.ReadTimeout):
                await asyncio.wait_for(
                    client.request("GET", "/hang", timeout=0.3), timeout=5
                )

# Testing Integrations

Integrations are testable without any infrastructure: tasks run inline, HTTP is mockable, state falls back to in-memory storage, and webhook fixtures replay real payloads.

## Run tasks inline

A `@integration_task` is a `TaskWrapper` — in tests, bypass the queue with `.run()`:

```python
import pytest

@pytest.mark.asyncio
async def test_sync_customer():
    result = await sync_customer.run("cus_123")
    assert result["status"] == "synced"
```

`@integration_step` functions are called directly (they run inline by design). Remember the footgun: `sync_customer("cus_123")` *enqueues* — always `.run()` in tests.

## Mock external APIs with respx

Clients are `httpx` underneath, so [respx](https://lundberg.github.io/respx/) mocks them cleanly:

```python
import respx
from httpx import Response

@respx.mock
@pytest.mark.asyncio
async def test_upsert_order():
    respx.put("https://erp.example.com/api/v1/orders/o-1").mock(
        return_value=Response(200, json={"id": "erp-9"})
    )
    result = await sync_order_to_erp.run(make_record("o-1"))
    assert result["erp_id"] == "erp-9"
```

Alternatively, [suppression rules](calling-external-apis.md#mock-or-block-endpoints) in a test environment's client YAML mock endpoints without touching test code — useful for running the whole app against canned responses.

## Construct clients directly

For unit tests of a client subclass, skip the registry and config directory:

```python
from flowstash.clients import ClientSettings

client = ErpClient(name="erp", settings=ClientSettings(
    client_id="erp", baseUrl="https://erp.example.com/api/v1"))
```

## Test webhook handlers through the app

Build the FastAPI app programmatically and drive it with httpx's ASGI transport — no server, no Docker:

```python
import httpx
from flowstash.config.runtime_config import (RuntimeConfig, WebhooksConfig,
                                             BackendConfig, BackendType)
from flowstash.runtime import create_fastapi_app

@pytest.mark.asyncio
async def test_order_webhook():
    config = RuntimeConfig(webhooks=WebhooksConfig(prefix="/webhooks"),
                           backend=BackendConfig(type=BackendType.ASYNC))
    app = create_fastapi_app(config, auto_import=[Path("src/api/routes")])
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/webhooks/shop/orders", json={"order": {...}})
    assert resp.status_code == 200
```

For realistic payloads, capture fixtures with `flowstash webhook listen` and replay them with `flowstash webhook test` — see [Handling Webhooks](handling-webhooks.md#capture-real-payloads).

## State in tests

With no store configured, `State` uses an in-memory SQLite fallback — isolated per process, gone after the test. Bind a context explicitly when testing outside a run:

```python
from flowstash.context import integration_context
from flowstash.integration import State

def test_cursor_logic():
    with integration_context(integration="shop", integration_pipeline="orders",
                             span_name="test", record_lifecycle=False):
        State.set("cursor", "2026-07-01")
        assert State.get("cursor") == "2026-07-01"
```

## Feeds in tests

Two options, by scope:

- **Unit**: call the consumer function directly with a hand-built `RecordData` — the decorator returns a callable wrapper.
- **End-to-end**: with the `asyncio` backend, `feed.publish()` invokes registered consumers in-process — publish and assert on effects. (The Redis-backed path can be tested against a local Redis; skip when unavailable, as the framework's own test suite does.)

## Quiet the observability noise

Observability defaults to `disabled` when no config is loaded, so tests are quiet by default. To assert on emitted events, configure the `console` or `local` store in a test fixture and inspect the output.

## Related

- [Handling Webhooks](handling-webhooks.md) — fixture capture and replay
- [Calling External APIs](calling-external-apis.md) — suppression rules

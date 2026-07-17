# Handling Webhooks

How to receive, secure, test, and evolve inbound webhooks. Concepts: [Scheduling & Ingress](../concepts/scheduling-and-ingress.md).

## Declare a handler

```python
from flowstash import ingress
from fastapi import Request

@ingress.webhook(integration="stripe", pipeline="payments",
                 path="/stripe/payments", method="POST")
async def stripe_webhook(request: Request):
    payload = await request.json()
    ...
    return {"received": True}
```

The route is served at `{webhooks.prefix}{path}` — with the default prefix, `POST /webhooks/stripe/payments`. Return values map naturally: `None` → 202, a FastAPI `Response` → passed through, anything else → JSON.

## Verify the sender

FlowStash does not authenticate inbound webhooks — the raw `Request` is yours precisely so you can verify signatures before parsing:

```python
import hmac, hashlib

@ingress.webhook(integration="shop", pipeline="orders", path="/shop/orders")
async def order_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Shop-Signature", "")
    expected = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return Response(status_code=401)
    payload = json.loads(body)
    ...
```

Keep the secret in the environment (`${SHOP_WEBHOOK_SECRET}` via `.env`) — see [Secrets & Configuration](managing-secrets-and-config.md).

## Keep handlers thin

Webhook senders enforce tight timeouts and retry on failure. Do the minimum inline — verify, validate, publish to a [feed](working-with-feeds.md) or `submit()` a [task](../concepts/tasks-and-steps.md) — and let the pipeline do the heavy lifting with its own retries:

```python
await RecordsFeed.get("orders").publish(RecordData(...))   # fast, durable
return {"received": True}                                   # sender is happy
```

This also makes duplicate deliveries (all webhook providers send them) harmless: the feed deduplicates by record.

## Capture real payloads

Guessing payload shapes from vendor docs is error-prone. The CLI can stand up a temporary public listener, capture real deliveries, and save one as a fixture:

```bash
flowstash webhook listen
```

Pick your webhook, point the vendor's webhook settings at the printed public URL, trigger a test event, select the captured payload — the CLI saves it under `tests/payloads/webhooks/` and offers to patch your decorator:

```python
from flowstash.ingress import FromFile

@ingress.webhook(..., test_payload=FromFile("tests/payloads/webhooks/shop-orders/2026-07-15.json"))
async def order_webhook(request: Request): ...
```

(Requires being logged in and the project linked to the platform.)

## Replay a fixture

With a `test_payload` attached, fire the captured request at your locally running API:

```bash
flowstash webhook test --path /shop/orders --target http://localhost:8000
```

The original method, headers, query, and body are reconstructed — a realistic end-to-end test in one command.

## Every delivery is traced

Each webhook invocation records a run with the incoming payload captured as a data exchange, so "did the webhook arrive, and what did it contain?" is answerable after the fact. See [Monitoring & Debugging](monitoring-and-debugging.md).

## Related

- [Working with Feeds](working-with-feeds.md) — where webhook data should usually go
- [Testing Integrations](testing-integrations.md)
- [CLI reference: webhook](../reference/cli.md#webhook)

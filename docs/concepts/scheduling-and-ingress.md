# Scheduling & Ingress

**Ingress** is how external events enter your integration. FlowStash supports the two fundamental shapes: **webhooks** (the other system pushes to you) and **polls** (you pull on a schedule). Both are declared with decorators and discovered automatically at boot.

## Webhooks

```python
from flowstash import ingress
from fastapi import Request

@ingress.webhook(integration="stripe", pipeline="payments",
                 path="/stripe/payments", method="POST")
async def stripe_webhook(request: Request):
    event = await request.json()
    ...
    return {"received": True}
```

- The decorator is **metadata-only**: it registers the handler for discovery without changing its behavior. The runtime builds a FastAPI route for each registered webhook.
- Routes are mounted under the configured prefix (`webhooks.prefix`, default `/webhooks`) — the handler above is served at `POST /webhooks/stripe/payments`.
- The handler receives the raw FastAPI `Request`. Returning `None` yields a `202`; a `Response` passes through; anything else is returned as JSON.
- Each request runs inside its own [execution context](context.md): a run is recorded, the incoming payload is captured as a data exchange, and an exception records a failed run and returns 500.
- `test_payload=FromFile("tests/payloads/...")` attaches a captured fixture for replay — the [`flowstash webhook`](../reference/cli.md#webhook) CLI can record real payloads and patch this in for you.

:::{admonition} Authenticate your webhooks
:class: warning
The framework does not authenticate inbound webhook requests. Validate the sender's signature (Stripe-Signature, HMAC, shared secret...) inside the handler before trusting the payload.
:::

## Polls

```python
@ingress.poll(integration="shopify", pipeline="orders",
              schedule="*/5 * * * *", name="poll_orders")
async def poll_orders(state: dict):
    since = state.get("cursor")
    orders = await fetch_orders_since(since)
    for order in orders:
        await orders_feed.publish(make_record(order))
    if orders:
        state["cursor"] = orders[-1]["updated_at"]
```

A poll is a specialized [task](tasks-and-steps.md) with two extras:

1. **A cron schedule** — a 5-field cron string or a `Schedule` object; the framework fires it for you.
2. **Durable cursor state** — a `state: dict` is injected as the first argument, loaded from the [state store](state.md) (`ingress` scope) and persisted **only if the handler succeeds**. A failed run retries the same window.

Because a poll is a `TaskWrapper`, you can also trigger it manually: `poll_orders.submit()` (state is still injected).

## Who fires the schedules

Declaring a schedule (`@ingress.poll(schedule=...)` or `@integration_task(default_schedule=...)`) registers intent; the configured backend executes it:

| Backend | Scheduler |
|---|---|
| `asyncio` | in-process APScheduler inside the API service |
| `dramatiq` | APScheduler inside the worker process, enqueuing to Dramatiq |
| `managed` | schedules are registered with the platform at deploy time; Cloud Scheduler fires them and Cloud Tasks pushes the work to your service |

The declaration is identical across backends — deploying the same code to managed infrastructure requires no changes.

Cron expressions are validated strictly: exactly 5 POSIX fields, optional `CRON_TZ=<zone>` prefix, no `@daily`-style macros.

## A typical ingress pipeline

Ingress handlers should stay thin: validate, then hand off.

```
webhook / poll  →  publish to a feed  →  feed consumers transform & deliver
                └→ or submit a task for one-shot work
```

This keeps the HTTP handler fast (webhooks time out!), makes the work retryable, and gives every stage its own trace.

## Related

- Guides: [Handling Webhooks](../guides/handling-webhooks.md) · [Scheduling Tasks](../guides/scheduling-tasks.md)
- [Tasks & Steps](tasks-and-steps.md) · [Feeds & Pipelines](feeds-and-pipelines.md) · [State](state.md)

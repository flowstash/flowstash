# Building an Integration

The end-to-end walkthrough. We'll build a realistic integration — syncing orders from a webhook-emitting shop into an ERP — using every major piece of FlowStash: ingress, feeds, clients, tasks, state, and observability. Each step links to the deeper guide for that topic.

**The scenario:** the shop POSTs a webhook when an order changes. We want the *latest state* of every order pushed into the ERP, resilient to bursts, retries, and ERP downtime.

## 0. Scaffold

```bash
flowstash init --name shop-sync
pip install -e ".[api,worker,dev]"
```

(Details: [Quickstart](../getting-started/quickstart.md), [Project Setup](../getting-started/project-setup.md).)

## 1. Model the record

`src/shared/models/models.py`:

```python
from pydantic import BaseModel

class Order(BaseModel):
    id: str
    status: str
    total: float
    updated_at: str
```

Pydantic models survive the feed round trip intact — the consumer receives a validated `Order`, not a dict.

## 2. Receive the webhook

`src/api/routes/webhooks.py`:

```python
from flowstash import ingress
from flowstash.pipelines import RecordData
from flowstash.pipelines.records_feed import RecordsFeed
from fastapi import Request
from shared.models.models import Order

@ingress.webhook(integration="shop", pipeline="orders",
                 path="/shop/orders", method="POST")
async def order_webhook(request: Request):
    payload = await request.json()
    # TODO: verify the shop's webhook signature before trusting the payload
    order = Order.model_validate(payload["order"])
    await RecordsFeed.get("orders").publish(RecordData(
        record_id=order.id,
        record_type="order",
        data=order,
        timestamp=None,          # defaults to now; drives latest-wins
    ))
    return {"received": True}
```

The handler stays thin: validate, publish, return. If the shop sends 50 updates for the same order in a minute, the feed compacts them — downstream sees only the latest state. (More: [Handling Webhooks](handling-webhooks.md), [Feeds & Pipelines](../concepts/feeds-and-pipelines.md).)

## 3. Define the ERP client

`config/shared/clients/erp.yaml`:

```yaml
client_id: erp
baseUrl: https://erp.example.com/api/v1
timeout: 30
auth:
  type: oauth2
  client_id: "${ERP_CLIENT_ID}"
  client_secret: "${ERP_CLIENT_SECRET}"
  token_url: https://erp.example.com/oauth/token
retry:
  maxRetries: 3
```

Put the secrets in `config/dev/.env` (never in the YAML). Add typed methods in `src/shared/clients/erp.py`:

```python
from flowstash.clients import HttpClient, client

@client("erp")
class ErpClient(HttpClient):
    async def upsert_order(self, order: dict) -> dict:
        resp = await self.request("PUT", f"/orders/{order['id']}", json=order)
        return resp.json()
```

Verify it works before writing any pipeline code:

```bash
flowstash client curl erp /orders -q "limit=1"
```

(More: [Calling External APIs](calling-external-apis.md).)

## 4. Consume and deliver

`src/worker/tasks/consumers.py`:

```python
from flowstash.pipelines import RecordData, feed_consumer
from flowstash.observability.logging import logger
from shared.clients.erp import ErpClient

@feed_consumer(feed_id="orders", subscription="erp-sync",
               debounce_delay_ms=2000)      # collapse bursts per order
async def sync_order_to_erp(record: RecordData):
    order = record.data                     # the Order model, reconstructed
    erp = ErpClient.get_client()
    result = await erp.upsert_order(order.model_dump())
    logger.info("synced order %s", order.id, extra={"erp_id": result.get("id")})
```

If the ERP is down, the handler raises, the record stays unacknowledged, and it is redelivered — with the *latest* payload, not a stale one. (More: [Working with Feeds](working-with-feeds.md).)

## 5. Add a reconciliation poll

Webhooks get lost. A poll with a durable cursor backfills anything missed:

`src/worker/tasks/reconcile.py`:

```python
from flowstash import ingress
from flowstash.pipelines import RecordData
from flowstash.pipelines.records_feed import RecordsFeed
from flowstash.clients import get_client
from shared.models.models import Order

@ingress.poll(integration="shop", pipeline="orders",
              schedule="*/15 * * * *", name="reconcile_orders")
async def reconcile_orders(state: dict):
    shop = get_client("shop")
    since = state.get("cursor", "1970-01-01T00:00:00Z")
    resp = await shop.request("GET", "/orders", params={"updated_since": since})
    orders = resp.json()["orders"]
    feed = RecordsFeed.get("orders")
    for raw in orders:
        order = Order.model_validate(raw)
        await feed.publish(RecordData(record_id=order.id, record_type="order",
                                      data=order))
    if orders:
        state["cursor"] = orders[-1]["updated_at"]   # persisted only on success
```

Both paths — webhook and poll — publish to the same feed, and dedup ensures the ERP consumer never does double work. (More: [Scheduling Tasks](scheduling-tasks.md), [State](../concepts/state.md).)

## 6. Run and observe

```bash
export ENVIRONMENT=dev
python api_main.py            # terminal 1
python worker_main.py         # terminal 2 (or: flowstash run dev)

curl -X POST localhost:8000/webhooks/shop/orders \
     -H 'Content-Type: application/json' \
     -d '{"order": {"id": "o-1", "status": "paid", "total": 99.5, "updated_at": "2026-07-15T12:00:00Z"}}'
```

Every hop is traced: the webhook run, the record's `PUBLISHED` link, the consumer run with its `CONSUMED` link, and the ERP HTTP exchange (with the OAuth token masked). With the `logfile` store, inspect `logs/observability/`; on the managed platform, follow the trace in the UI. (More: [Monitoring & Debugging](monitoring-and-debugging.md).)

## 7. Ship it

```bash
flowstash env add prod        # choose backend: managed
flowstash link                # bind the project to the platform
flowstash deploy              # build + deploy, schedules registered automatically
```

The code you wrote doesn't change: in production, feed delivery, scheduling, and retries move to the managed platform's infrastructure. Self-hosting instead? Choose the `dramatiq` backend and deploy the compose stack. (More: [Deployment overview](../deployment/overview.md).)

## What you used

| Piece | Where |
|---|---|
| `@ingress.webhook` | receive pushes |
| `RecordsFeed` + `RecordData` | decouple, dedupe, latest-wins |
| Client YAML + `@client` subclass | ERP with OAuth2, retries |
| `@feed_consumer` | reliable delivery with debouncing |
| `@ingress.poll` + injected `state` | reconciliation with a durable cursor |
| Observability | tracing every hop for free |

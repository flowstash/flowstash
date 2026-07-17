# Feeds & Pipelines

A **record feed** is FlowStash's pub/sub channel for business data. Producers publish records into a named feed; any number of consumers subscribe to it independently. Feeds decouple the systems in an integration: a webhook can publish an order the moment it arrives, and one consumer syncs it to the ERP while another updates a search index — neither knows about the other.

Feeds are not plain message queues. Each feed is a **compacted, deduplicated, latest-wins log**:

- Every record has a **dedupe key** (explicit, or derived as `integration:record_type:record_id`).
- Publishing a newer record for the same key replaces the snapshot; an older timestamp is ignored.
- Consumers always receive the **current snapshot** for a key, not the historical payload of each individual publish.

This matches the reality of integration work: what usually matters is the latest state of order `#123`, not every intermediate version — and bursts of updates to the same record collapse instead of flooding downstream systems.

## Records

```python
from flowstash.pipelines.records_model import RecordData

record = RecordData(
    record_id="123",          # business identifier
    record_type="order",      # logical category
    data=order,               # dict, Pydantic model, dataclass, bytes...
    # timestamp=...           # event time; defaults to now (drives latest-wins)
    # dedupe_key=...          # defaults to integration:record_type:record_id
)
```

Provenance fields (`source_integration`, `source_pipeline`, `source_run_id`, `source_traceparent`) are filled in automatically from the ambient execution context — every record knows which run produced it.

Serialization is JSON with a typed envelope: pass a Pydantic model as `data` and the consumer receives the same model class, reconstructed and validated. If the class is no longer importable on the consumer side (say, after a deploy), delivery degrades gracefully to the raw dict instead of crashing. Payloads over the inline threshold (5 KB on the Redis backend, 1 MB on managed) are transparently offloaded to blob storage and resolved back on delivery.

## Publishing

```python
from flowstash.pipelines.records_feed import RecordsFeed

feed = RecordsFeed.get("orders")
await feed.publish(record)
```

A feed is identified purely by its string `feed_id` — there is no registration step. Publishing also emits a `PUBLISHED` record link into observability, so you can later ask "which run published this record?"

## Consuming

```python
from flowstash.pipelines.consumer import feed_consumer
from flowstash.pipelines.records_model import RecordData

@feed_consumer(feed_id="orders", subscription="erp-sync")
async def sync_to_erp(record: RecordData):
    ...

@feed_consumer(feed_id="orders", subscription="indexer",
               batch=True, max_batch_size=100, max_delay_ms=500)
async def index_orders(records: list[RecordData]):
    ...
```

Key semantics:

- **Subscriptions fan out.** Each distinct `subscription` name keeps its own cursor over the feed — both consumers above receive every record. A new subscription starts from the beginning of the retained stream; existing ones resume where they left off.
- **Batch vs classic.** `batch=True` delivers lists (up to `max_batch_size`, held for at most `max_delay_ms` to fill); classic mode delivers one record per call. Sync and async handlers both work.
- **Debouncing** (classic mode only): `debounce_delay_ms` collapses a burst of updates sharing a dedupe key into a single delivery of the latest payload once arrivals quiet down, bounded by `max_debounce_window_ms`.
- **At-least-once delivery.** Records are acknowledged only after the handler returns; a raised exception leaves them pending for redelivery.
- Each delivery runs inside its own execution context and emits a `CONSUMED` record link.

:::{admonition} Managed-mode policies
:class: note
`rate_limit_per_sec`, `concurrency`, `debounce_*`, and `schedule` are declared on the decorator but **enforced by the managed platform**, which receives the consumer specs at deploy time. The local development runner delivers records but does not enforce these policies.
:::

## Backends

The feed transport follows your runtime backend (see [Architecture](architecture.md)):

| Backend | Transport | Notes |
|---|---|---|
| `asyncio` | in-process | consumers invoked directly on publish; no Redis needed; dev only |
| `dramatiq` | Redis streams | compaction via an atomic Lua script; consumer groups per subscription |
| `managed` | platform API | publish via `POST /v1/feed/…`; dedup, batching, and policies enforced server-side; consumers run as managed jobs |

The programming model — `RecordsFeed.publish()` and `@feed_consumer` — is identical across all three; only configuration changes.

## Feeds vs tasks

Feeds and [tasks](tasks-and-steps.md) are complementary:

- **Feeds** carry *data*: multi-subscriber, deduplicated, latest-wins.
- **Tasks** carry *work*: one-shot or scheduled job submission with a single executor.

A common pipeline shape: an ingress webhook publishes to a feed → a feed consumer transforms records and calls a [client](clients.md) → the consumer submits follow-up tasks for slow side effects.

## Related

- Guide: [Working with Feeds](../guides/working-with-feeds.md)
- Concepts: [Tasks & Steps](tasks-and-steps.md) · [Scheduling & Ingress](scheduling-and-ingress.md) · [Observability](observability.md)

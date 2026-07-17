# Working with Feeds

Practical recipes for record feeds. For the mental model (compaction, latest-wins, subscriptions), read [Feeds & Pipelines](../concepts/feeds-and-pipelines.md) first.

## Publish records

```python
from flowstash.pipelines import RecordData
from flowstash.pipelines.records_feed import RecordsFeed

feed = RecordsFeed.get("orders")
await feed.publish(RecordData(
    record_id=order.id,
    record_type="order",
    data=order,                     # dict or Pydantic model
))
```

Guidelines:

- **Use event time when you have it.** `timestamp` drives latest-wins ordering; if the source system provides an update time, pass it — otherwise an out-of-order webhook could overwrite newer data with older data.
- **Pass Pydantic models as `data`.** They arrive at the consumer reconstructed and validated; schema drift degrades gracefully to a dict instead of crashing delivery.
- **Custom dedupe keys** when identity spans types: `dedupe_key="customer:42"` overrides the default `integration:record_type:record_id`.
- Payloads above the inline threshold (5 KB local Redis, 1 MB managed) are automatically offloaded to blob storage — publish freely, but keep records *record-sized*; ship bulk files another way.

## Consume one at a time

```python
from flowstash.pipelines import RecordData, feed_consumer

@feed_consumer(feed_id="orders", subscription="erp-sync")
async def sync_order(record: RecordData):
    ...
```

- Name the `subscription` explicitly. It defaults to the function's module path, which means **renaming the function or module silently creates a new subscription** that replays the feed from the beginning.
- Raise to retry: unacknowledged records are redelivered. Make handlers idempotent — delivery is at-least-once, and you always get the latest snapshot for the key.

## Consume in batches

```python
@feed_consumer(feed_id="orders", subscription="warehouse-export",
               batch=True, max_batch_size=100, max_delay_ms=500)
async def export_orders(records: list[RecordData]):
    ...
```

Delivery waits up to `max_delay_ms` to fill a batch of `max_batch_size`. Use batches when the destination has a bulk endpoint or per-call overhead dominates.

## Debounce chatty sources

```python
@feed_consumer(feed_id="orders", subscription="erp-sync",
               debounce_delay_ms=2000, max_debounce_window_ms=30000)
async def sync_order(record: RecordData):
    ...
```

A burst of updates to the same record collapses into one delivery of the final state, fired once the key has been quiet for `debounce_delay_ms` (force-delivered at `max_debounce_window_ms`). Classic mode only — it can't be combined with `batch=True`.

## Fan out to multiple consumers

Different `subscription` names on the same feed each receive every record independently:

```python
@feed_consumer(feed_id="orders", subscription="erp-sync")
async def to_erp(record: RecordData): ...

@feed_consumer(feed_id="orders", subscription="analytics")
async def to_analytics(record: RecordData): ...
```

One consumer failing or lagging never blocks the other.

## Replay a feed

A **new** subscription name starts from the beginning of the retained stream — the simplest replay is to point a fresh subscription at the feed:

```python
@feed_consumer(feed_id="orders", subscription="erp-sync-replay-2026-07")
async def backfill(record: RecordData): ...
```

Remember the compaction semantics: you replay the latest state per record, not the full history of every intermediate update.

## Know what's enforced where

`rate_limit_per_sec`, `concurrency`, and the debounce parameters are part of the consumer's declaration and are **enforced by the managed platform** in managed deployments (consumer specs are synced at deploy time). The local development runner delivers records and honors batching, but does not enforce rate or concurrency limits — don't rely on them in local load tests.

## Trace a record

Every publish and every consume emits a **record link** into observability, keyed by the record. To answer "what happened to order o-42?", query the record's links on the managed platform: you'll see the run that published it and every subscription's run that consumed it. See [Monitoring & Debugging](monitoring-and-debugging.md).

## Related

- [Feeds & Pipelines](../concepts/feeds-and-pipelines.md) — semantics
- [Building an Integration](building-an-integration.md) — feeds in a full pipeline

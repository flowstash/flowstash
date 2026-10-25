# Monitoring & Debugging

How to see what your integrations did — and work out why they didn't. The model behind all of this is described in [Observability](../concepts/observability.md).

## Set up a store

Nothing is recorded until you pick a store in `observability.yaml`:

::::{tab-set}
:::{tab-item} Local development
```yaml
# config/dev/observability.yaml
storeType: local
localStorePath: logs/observability
logging:
  enabled: true
  minLevel: INFO
```

Each run becomes a directory `logs/observability/{date}_{run_id}/` of JSON files — run events, spans, logs, data exchanges. `storeType: console` prints events to stdout instead.
:::
:::{tab-item} Managed
```yaml
# config/prod/observability.yaml
storeType: managed
managedApiKey: ${FLOWSTASH_API_KEY}
logging:
  enabled: true
  minLevel: INFO
```

Create the key with `flowstash api-keys new --scope observability:ingest --env prod`. Runs, spans, logs, data exchanges, and record links are queryable on the platform.
:::
::::

## Log with structure

```python
from flowstash.observability.logging import logger

logger.info("synced order %s", order_id, extra={"erp_id": erp_id, "attempt": 2})
```

Inside a run, the line lands in the run's trace with `erp_id` and `attempt` as queryable attributes. Third-party library logs and even `print()` output during a run are captured too — you rarely lose context to a library that "logs elsewhere".

Tune the firehose with `logging.minLevel`, `includePrefixes` / `excludePrefixes` (logger-name filters), and `passthrough: false` to stop mirroring captured logs to stdout (`FLOWSTASH_LOG_PASSTHROUGH` overrides per process).

## Label runs with records

Feed publish and consume link records to runs on their own. Code that calls APIs directly — a poll that syncs inventory, a webhook for one order — should say which records it touched. Then the run's **Records** tab lists them, and searching runs by record key finds this run too.

**One record at a time** — wrap the per-item work in `record_scope`. It links the record and tags every log inside the block with `fw.record_key`:

```python
from flowstash.records import record_scope

async def push(upc: str) -> None:
    async with sem:
        with record_scope(upc, record_type="upc"):
            await tc.reset_external_ats(upc=upc, qty=qty)
```

The record is linked when the block starts, so failed items show up too. By default no span is created; pass `span=True` for a span per record (small volumes only).

**Many records at once** — call `link_records` after the bulk work:

```python
from flowstash.records import link_records

results = await asyncio.gather(*(push(u) for u in upcs), return_exceptions=True)
ok = [u for u, r in zip(upcs, results) if not isinstance(r, BaseException)]
accepted = link_records(ok, record_type="upc")
if accepted < len(ok):
    logger.info("linked %d of %d records (limit or duplicates)", accepted, len(ok))
```

**The record follows the work** — a task submitted inside `record_scope` starts with the same current record and links it to its own run (with `source="run:<parent run id>"`):

```python
@ingress.webhook(integration="shop", pipeline="orders", path="/orders")
async def on_order(request: Request):
    order = await request.json()
    with record_scope(order["id"], record_type="order"):
        process_order.submit(order)   # child run is linked to shop:order:<id>
```

Pass `propagate=False` when the submitted tasks are not about that record. `link_records` never passes anything on.

**Limits**

- Keys are `integration:record_type:record_id` — the same as feed keys. Pass `key=` for a custom one.
- At most 5000 `LINKED` links per run (`FLOWSTASH_RECORD_LINKS_MAX_PER_RUN`). Extra links are dropped with one warning log; `record_scope` still tags logs past the limit.
- The same key twice in a run is linked once.
- The run's end event carries `fw.record_links_accepted` and `fw.record_links_dropped`.
- Helpers never block, never raise, and do nothing outside a run.

## Debugging playbook

**A run failed.** Open the run: the failing span carries the `error_summary`; the last data exchange shows the HTTP status and (masked) response body of the external call that broke. Failed client requests also log a masked, copy-pasteable `curl -v` command — reproduce the exact request from your terminal.

**A record didn't arrive.** Follow its record links: a `PUBLISHED` link with no `CONSUMED` link from your subscription means delivery is the problem (consumer not registered, subscription renamed, handler raising before completion). No `PUBLISHED` link at all means the producer never ran — check the webhook's runs.

**Which run touched record X?** Filter runs by `record_key`. Feeds, `record_scope`, and `link_records` all write the same key, so one search covers publishers, consumers, and plain tasks.

**A schedule didn't fire.** Look for runs at the expected timestamps. None? For `asyncio`/`dramatiq`, confirm the process hosting the scheduler was up; for managed, check the schedule list printed by `flowstash deploy` — schedules register at deploy time, so a task added without redeploying never fires.

**Work happened twice.** Delivery is at-least-once everywhere. Find both runs and compare `parent_run_id`/`operation_id`: same operation → a retry (make the handler idempotent); different operations → two genuine triggers (often webhook + reconciliation poll racing — feed dedup usually absorbs this).

**A delegated task vanished.** In the parent run, the `delegate:<task>` span shows whether submission succeeded. From there, the child run carries `parent_run_id` — if no child run exists, the queue accepted but never executed: check the worker's logs and (managed) the platform's task queue.

## Correlate across systems

Every run carries W3C `trace_id`/`span_id`, propagated over queue hops. If your other services use OpenTelemetry, FlowStash spans slot into the same distributed trace. For pinpointing a specific entity, prefer record links (`record_key`) over text-searching logs; the record key also follows submitted tasks.

## Durability knobs

Ingestion is asynchronous and batched (`durability: eventual`); events flush after each task and on graceful shutdown. If a process is killed hard, the tail of a run's events can be lost — for critical short-lived jobs, `durability: immediate` awaits every write at a latency cost. Batching/retry behavior is tunable via `FLOWSTASH_OBS_*` environment variables (see the [Configuration reference](../reference/configuration.md)).

## Related

- [Observability](../concepts/observability.md) — the data model
- [Working with Feeds](working-with-feeds.md#trace-a-record) — record traceability

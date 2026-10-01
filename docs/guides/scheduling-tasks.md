# Scheduling Tasks

Recurring work in FlowStash comes in two flavors: **scheduled tasks** (run this function on a cron) and **polls** (fetch new data on a cron, with a durable cursor). This guide covers both, plus one-off delayed execution.

## Recurring task

```python
from flowstash.decorators import integration_task
from flowstash.queue.backend import Schedule

@integration_task(integration="reports", integration_pipeline="daily",
                  default_schedule=Schedule(cron="0 6 * * *"))
async def daily_report():
    ...
```

The schedule registers at import time; the configured backend fires it (in-process scheduler for `asyncio`/`dramatiq`, Cloud Scheduler via the platform for `managed` — registered automatically at deploy).

Cron rules: strict 5-field POSIX (`min hour dom month dow`), optional `CRON_TZ=Europe/Prague` prefix, **no** `@daily`-style macros.

## Poll with a cursor

When the recurring job is "fetch what's new since last time", use `@ingress.poll` instead — it persists your cursor for you:

```python
from flowstash import ingress

@ingress.poll(integration="shopify", pipeline="orders",
              schedule="*/5 * * * *", name="poll_orders")
async def poll_orders(state: dict):
    since = state.get("cursor")
    new = await fetch_since(since)
    ...
    if new:
        state["cursor"] = new[-1]["updated_at"]
```

The injected `state` dict is loaded from the [state store](../concepts/state.md) and saved **only when the handler returns successfully** — a failed run retries the same window, so progress is never lost and gaps never open. Design the fetch to be idempotent (feeds downstream make this easy — duplicates deduplicate).

## One-off delayed execution

```python
from datetime import datetime, timedelta, timezone

send_reminder.schedule(datetime.now(timezone.utc) + timedelta(hours=24), user_id)
```

Prefer passing a `datetime`. (A bare number is currently interpreted as **milliseconds** despite the docstring saying seconds — avoid the ambiguity.) Note the `asyncio` dev backend doesn't support delayed execution; use `dramatiq` or `managed`.

## Trigger a scheduled task manually

Every scheduled task is still a normal task:

```python
await daily_report.run()      # inline, e.g. from a test
daily_report.submit()         # enqueue now, ahead of schedule
```

## Operational notes

- **Where schedules run:** `asyncio` — inside the API process (disable with `backend.async.enable_scheduled_jobs: false` in config, or `FLOWSTASH_ASYNC_SCHEDULED_ENABLE=false`); `dramatiq` — inside the worker process; `managed` — on the platform. Deploying to managed prints the registered schedules (`flowstash deploy` output lists task name + cron).
- **Overlaps:** the scheduler fires on the cron regardless of whether the previous run finished. If a run can outlast its interval, guard with [state](../concepts/state.md) or make the work idempotent.
- **Observability:** each firing is a run like any other — a silent schedule (no runs recorded at the expected times) is your signal that the scheduler or deploy registration is broken. See [Monitoring & Debugging](monitoring-and-debugging.md).

## Related

- [Scheduling & Ingress](../concepts/scheduling-and-ingress.md) · [Tasks & Steps](../concepts/tasks-and-steps.md)
- [State](../concepts/state.md) — how poll cursors persist

# Tasks & Steps

Tasks and steps are the units of work in FlowStash. Both wrap a plain Python function with an execution context and observability; the difference is *where* they run.

- **`@integration_step`** — runs inline, right where it's called. Use it to structure a run into named, traceable phases.
- **`@integration_task`** — can be submitted to the task backend (a queue) or run inline. Use it for work that should execute elsewhere: on a worker, later, or on a schedule.

```python
from flowstash.decorators import integration_task, integration_step
from flowstash.queue.backend import Schedule

@integration_step(integration="stripe", integration_pipeline="sync_customers")
def normalize(record: dict) -> dict:
    return {"id": record["id"], "email": record["email"].lower()}

@integration_task(
    integration="stripe",
    integration_pipeline="sync_customers",
    default_schedule=Schedule(cron="0 * * * *"),   # also run hourly
    tags={"team": "billing"},
)
async def sync_customer(customer_id: str):
    ...
```

Both decorators accept sync and async functions and preserve the function's nature — an async step must still be awaited.

## Runs and spans

Every execution is recorded in [observability](observability.md) following one automatic rule:

- If **no run is active**, the step/task starts a new **run** (a root trace with its own `run_id`).
- If called **inside an active run**, it records a nested **span** within that run.

You never manage this explicitly — call a step from a webhook handler and it becomes a span of the webhook's run; call it from a bare script and it becomes its own run.

## The TaskWrapper API

`@integration_task` returns a `TaskWrapper`, not a plain function. It exposes:

| Call | What happens |
|---|---|
| `task.submit(*args, **kwargs)` | Enqueue on the configured backend. Returns a `JobHandle`. |
| `task(*args, **kwargs)` | **Alias for `submit`** — also enqueues. |
| `await task.run(*args, **kwargs)` | Execute immediately, in-process. |
| `task.schedule(eta, *args, **kwargs)` | Enqueue for later — pass a `datetime` for an absolute time. |

:::{admonition} Calling a task enqueues it
:class: warning
`sync_customer("cus_123")` does **not** run the function — it submits it to the queue and returns a `JobHandle`. To execute inline (in tests, or when composing tasks), use `await sync_customer.run("cus_123")`.
:::

:::{admonition} Numeric delays
:class: note
`schedule()` also accepts a numeric delay, but the unit is currently interpreted as **milliseconds** (the docstring says seconds). Prefer passing an explicit `datetime` until this is settled.
:::

### JobHandle

`submit()` and `schedule()` return a `JobHandle`:

```python
handle = sync_customer.submit("cus_123")
handle.id            # backend job id
handle.status()      # e.g. "running", "finished", "submitted"
await handle.result(timeout=30)   # asyncio backend only
handle.cancel()
```

Capabilities vary by backend: the local `asyncio` backend supports awaiting results and cancellation; the managed backend is fire-and-forget (`result()` raises, `cancel()` returns `False`) — results there are observed through [observability](observability.md), not return values.

## Delegation is traced

When a task submits another task, FlowStash records a **delegation span** (`delegate:<task>`, status `DELEGATED`) in the parent run and threads `parent_run_id` and an `operation_id` into the child's context. In the observability model, the parent run, the delegation, and the child run form one connected trace — you can follow a piece of work across process and machine boundaries.

## Scheduled tasks

Give a task a `default_schedule` and it is registered as a recurring job at import time:

```python
@integration_task(integration="reports", default_schedule=Schedule(cron="0 6 * * *"))
async def daily_report():
    ...
```

`Schedule` validates a strict 5-field POSIX cron expression (no `@daily` macros). Who fires the schedule depends on the backend: an in-process scheduler for `asyncio` and `dramatiq`, or Cloud Scheduler via the platform in `managed` mode. For pull-based ingestion with persisted cursor state, prefer [`@ingress.poll`](scheduling-and-ingress.md), which builds on the same machinery.

## Choosing between the pieces

| You need | Use |
|---|---|
| Structure within one run, traceable phases | `@integration_step` |
| Work on a worker / later / retried independently | `@integration_task` + `.submit()` |
| Recurring job | `default_schedule` or [`@ingress.poll`](scheduling-and-ingress.md) |
| Reacting to data records with fan-out | [`@feed_consumer`](feeds-and-pipelines.md) |

## Related

- [Execution Context](context.md) — what's ambient inside a task
- [Scheduling & Ingress](scheduling-and-ingress.md) · [Observability](observability.md)
- Guide: [Building an Integration](../guides/building-an-integration.md)

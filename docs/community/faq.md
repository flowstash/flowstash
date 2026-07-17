# FAQ

## General

**How is FlowStash different from Celery/Dramatiq/a job queue?**
A job queue is one of FlowStash's building blocks, not the product. FlowStash adds the integration-specific layer: declarative API clients with auth and retries, webhook/poll ingress, deduplicated latest-wins record feeds, scoped durable state, and correlated observability across all of it. (In fact, the self-hosted backend *uses* Dramatiq underneath.)

**How does it compare to workflow engines like Prefect, Dagster, or Temporal?**
Those orchestrate *your* long-running workflows and DAGs. FlowStash is purpose-built for *system-to-system integration*: event-shaped, record-centric, always-on. If your problem is "keep system A and system B in sync forever", FlowStash's feed/ingress model fits more directly than a DAG.

**Do I need the managed platform?**
No. The `asyncio` and `dramatiq` backends are fully self-contained — the framework is useful with nothing but your own Redis. The managed platform takes over the operational pieces (queueing, scheduling, feed storage, observability querying, deploys) when you don't want to run them.

## Programming model

**Why did `my_task(...)` return a JobHandle instead of running?**
Calling a `@integration_task` **submits** it to the queue (it's an alias for `.submit()`). Run it inline with `await my_task.run(...)`. This is the most common first-day surprise.

**Why does my feed consumer receive fewer records than I published?**
By design. Feeds are compacted: multiple publishes with the same dedupe key collapse to the latest state. If you need every event rather than the latest state, give each event a unique `record_id` (or `dedupe_key`).

**Why did my consumer replay the whole feed from the beginning?**
The `subscription` name defaults to the function's module path — renaming the function or moving the module creates a *new* subscription, which starts from the beginning. Always set `subscription=` explicitly.

**Is delivery exactly-once?**
At-least-once, everywhere — design handlers to be idempotent. On the managed backend you can additionally enable the lease broker for exactly-once task execution under retries.

**Can I use my own FastAPI routes alongside webhooks?**
Yes — any module under `src/api/routes/` exposing a `router` is mounted automatically into the API app.

## Operations

**Are webhook endpoints authenticated?**
No — verify the sender's signature inside your handler. The raw `Request` is passed through untouched for exactly this reason.

**What happens to observability data if the store is down?**
Ingestion is asynchronous with retries; your integration never fails because observability failed. Run-completion events get an aggressively larger retry budget so runs don't get stuck showing "running".

**Where do secrets live?**
In environment variables — populated locally from gitignored `.env` files, injected as service env vars in production — and referenced from YAML as `${VAR}`. Config files never contain secrets. See [Secrets & Configuration](../guides/managing-secrets-and-config.md).

**Which Python versions are supported?**
3.11 and newer.

## Still stuck?

Open a [GitHub issue](https://github.com/flowstash/flowstash/issues) — bug reports and docs gaps are equally welcome.

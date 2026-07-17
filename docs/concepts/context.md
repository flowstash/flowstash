# Execution Context

Every piece of FlowStash code runs inside an **execution context** — an ambient, immutable object that identifies the current run and carries its trace identity. The framework creates and propagates it automatically; you mostly just read from it.

```python
from flowstash.context import current_context

ctx = current_context()      # None when outside any run
ctx.run_id                   # unique id of this run
ctx.integration              # e.g. "stripe"
ctx.integration_pipeline     # e.g. "sync_customers"
ctx.trace_id, ctx.span_id    # W3C trace identity (from the traceparent)
ctx.tags                     # tags set on the task/step
ctx.current_record_key       # set while processing a feed record
ctx.parent_run_id            # the run that submitted this one, if delegated
```

## Where contexts come from

You rarely construct a context yourself. One is opened for you by:

- `@integration_task` / `@integration_step` — a new run, or a nested span when a run is already active (see [Tasks & Steps](tasks-and-steps.md))
- webhook and poll handlers ([Scheduling & Ingress](scheduling-and-ingress.md))
- feed consumer deliveries ([Feeds & Pipelines](feeds-and-pipelines.md))
- the worker, for each executing job

For manual scripts and tests you can open one explicitly:

```python
from flowstash.context import integration_context

with integration_context(integration="stripe", integration_pipeline="sync",
                         span_name="backfill"):
    ...
```

The same automatic rule applies: no active run → this records a run; already inside one → it records a span.

## What the context powers

The context is the thread that ties the framework together:

- **Observability** — every run event, span, log line, and data exchange is stamped with the context's correlation (run id, trace/span ids, integration, pipeline, tags). See [Observability](observability.md).
- **Logging** — `from flowstash.observability.logging import logger`; inside a context, `logger.info(...)` is captured into the run's trace (extra kwargs become structured attributes); outside one, it falls through to standard Python logging.
- **State scoping** — `State.get`/`set` resolve their namespace (`integration`, `pipeline`, or `ingress`) from the current context. See [State](state.md).
- **Record provenance** — records published to a feed automatically capture the source integration, pipeline, run id, and traceparent from the context.
- **Delegation** — when a task submits another task, the child context carries `parent_run_id` and an `operation_id`, linking the two runs into one trace across processes and machines.

## Propagation across boundaries

Contexts propagate as W3C `traceparent` headers plus FlowStash metadata in queue message headers. Whether the child work runs in the same process (asyncio backend), on a Dramatiq worker thread, or in a Cloud Run container on the managed platform, it gets a fresh `run_id` of its own while staying causally linked to its parent.

## Related

- [Tasks & Steps](tasks-and-steps.md) · [State](state.md) · [Observability](observability.md)

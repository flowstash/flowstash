# Observability

Integrations fail in the gaps between systems — a webhook that never arrived, an API that returned 500 at 3 a.m., a record that silently didn't sync. FlowStash treats observability as a first-class subsystem: every execution is recorded automatically, without instrumentation code in your integrations.

## The data model

| Entity | What it records |
|---|---|
| **Run** | one end-to-end execution: entry point, start/finish, status (`RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`) |
| **Span** | a named phase within a run — each `@integration_step`, nested task, or delegation |
| **Log** | log lines captured while the run was active, with structured attributes |
| **Data exchange** | one interaction with an external system: HTTP method, status code, duration, masked headers, request/response payloads |
| **Record link** | a business record's connection to a run: `PUBLISHED` or `CONSUMED` |

Everything is stitched together by a shared **correlation**: `run_id`, OTel-style `trace_id`/`span_id`, `parent_run_id`/`operation_id` for delegated work, plus integration, pipeline, project, environment, and tags. When task A submits task B on another machine, their runs share a causal chain you can follow in one trace.

## What gets recorded, automatically

- Every task, step, webhook, poll, and feed delivery opens a run or span (see [Tasks & Steps](tasks-and-steps.md)).
- Every request through a [client](clients.md) emits a data exchange — with `Authorization` headers, cookies, and secret-looking fields **masked**, and large payloads offloaded to blob storage.
- Every feed publish/consume emits a record link, so you can ask *"which runs touched order #123?"* — end-to-end record traceability across integrations.
- Task delegation emits a `DELEGATED` span linking parent and child runs.

## Logging

```python
from flowstash.observability.logging import logger

logger.info("synced %s", customer_id, extra={"amount": 42})
```

Inside a run, log lines are captured into the run's trace with their structured attributes (and by default also pass through to normal Python logging). Outside a run, they fall back to standard logging. Global capture goes further: root-logger records and even bare `print()` output emitted during a run are attached to it — third-party library logs included.

## Store backends

Where the data goes is configured in `observability.yaml`:

```yaml
storeType: managed          # disabled | console | local | managed
managedApiKey: ${FLOWSTASH_API_KEY}
logging:
  enabled: true
  minLevel: INFO
```

| `storeType` | Destination |
|---|---|
| `disabled` (default) | nothing recorded |
| `console` | printed to stdout — quick local visibility |
| `local` | JSON files under `localStorePath`, one directory per run (`{date}_{run_id}/`) |
| `managed` | the FlowStash platform's ingestion API, authenticated with an [API key](../reference/cli.md#api-keys) scoped to `observability:ingest` |

The managed store batches events in a background thread and retries on failure — terminal run-completion events get an aggressively higher retry budget so a run never appears permanently "running". Ingestion is fire-and-forget by default (`durability: eventual`); `immediate` awaits every write.

Querying runs, spans, logs, and record links happens through the managed platform — the library itself is write-only. With the `local` store, inspect the JSON files directly.

## Performance characteristics

Recording happens off the hot path: events are enqueued to background workers, batched, and flushed on task exit and graceful shutdown (`flushOnTaskExit: true` by default). A failing observability pipeline never fails your integration — capture errors are swallowed by design.

## Related

- Guide: [Monitoring & Debugging](../guides/monitoring-and-debugging.md)
- [Execution Context](context.md) — where correlation comes from
- [Configuration reference](../reference/configuration.md)

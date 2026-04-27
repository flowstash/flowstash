# flowstash Lib

`flowstash` is the core library for building integrations in the flowstash Framework. It provides decorators, context management, HTTP clients, observability tooling, and task queue abstractions that enable you to build robust, observable, and scalable integration pipelines.

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
  - [Integration Context](#integration-context)
  - [Pipelines & Ingress](#pipelines--ingress)
  - [Decorators](#decorators)
  - [Task Execution Modes](#task-execution-modes)
- [Modules](#modules)
  - [Context (`context.py`)](#context)
  - [Ingress (`pipelines/ingress.py`)](#ingress)
  - [RecordsFeed (`pipelines/records_feed.py`)](#recordsfeed)
  - [Decorators (`decorators.py`)](#decorators-1)
  - [Clients](#clients)
  - [Queue Backends](#queue-backends)
  - [Configuration](#configuration)
  - [Observability](#observability)
- [Usage Examples](#usage-examples)
- [Architecture](#architecture)

---

## Installation

[TBD]

---

## Core Concepts

### Integration Context

The `IntegrationContext` is a central context that is automatically propagated through all operations in an integration pipeline. It carries:

- **`integration`**: The name of the integration (e.g., `"salesforce"`, `"sap"`)
- **`integration_pipeline`**: The specific pipeline or process being executed
- **`run_id`**: A unique identifier for the current execution run
- **`current_record_key`**: Identity of the record currently being processed (e.g. `integration:type:id`)
- **`traceparent` / `tracestate`**: OpenTelemetry W3C trace context for distributed tracing
- **`tags`**: Metadata tags attached to spans and events

### Pipelines & Ingress

The framework distinguishes between **Ingress** (bringing data in) and **Consumption** (processing data).

1. **Webhook Ingress**: Metadata-only decoration for HTTP handlers. Safe to stack with any router.
2. **Polling Ingress**: Scheduled entrypoints that own durable **state** (e.g. watermarks).
3. **RecordsFeed**: The opinionated queue for record data. Handles deduplication and "latest-wins" semantics.
4. **Feed Consumers**: Batch-aware handlers for processing records from a feed.

---

## Modules

### Context

**File:** `context.py`

Provides the `IntegrationContext` dataclass and utilities for managing context flow.

```python
from flowstash.context import (
    IntegrationContext,      # The context dataclass
    current_context,         # Get the current context
    integration_context,     # Context manager for scoped context
)
```

---

### Ingress

**File:** `pipelines/ingress.py`

#### `@ingress.webhook`

Attaches integration metadata to a function without wrapping it.

```python
@router.post("/webhook/slack")
@ingress.webhook(pipeline="slack.messages", integration="slack")
async def handler(request):
    data = await request.json()
    feed = RecordsFeed.get(feed_id="slack.messages")
    feed.publish(RecordData(
        record_id=data["id"],
        record_type="slack.message",
        data=data,
        timestamp=parse_ts(data.get("updated_at")),
    ))
```

#### `@ingress.poll`

Scheduler entrypoint that injects a durable `state` dict.

```python
@ingress.poll(pipeline="slack.messages", integration="slack", schedule="*/5 * * * *")
async def poll_slack(state: dict):
    since = state.get("since")
    page = await slack.fetch_messages(since=since)
    
    feed = RecordsFeed.get(feed_id="slack.messages")
    for msg in page.items:
        feed.publish(RecordData(record_id=msg["id"], record_type="message", data=msg))
    
    state["since"] = page.new_since # Automatically saved on success
```

---

### RecordsFeed & Consumers

**Files:** `pipelines/records_feed.py`, `pipelines/consumer.py`

#### `RecordsFeed.publish`

Publishes a `RecordData` object. Automatically emits a `RecordLink` event.
Deduplication is handled by `record_key = (integration, record_type, record_id)`.
If `timestamp` is provided, only the latest record per key is kept.

#### `@feed_consumer`

Wraps a function to process records from a feed with batching and rate-limiting support.

```python
@feed_consumer(
  feed_id="slack.messages",
  batch=True,
  max_batch_size=200,
  max_delay_ms=500,
  rate_limit_per_sec=50,
  concurrency=10,
)
async def process_messages(records: list[RecordData]):
    for record in records:
        # Business logic here
        pass
```

---

### Observability

**Directory:** `observability/`

#### Record Tracking (`RecordLink`)

The framework has moved away from embedding `record_ids` in Span/Run events. Instead, it uses a first-class **RecordLink** stream.

- **`PUBLISHED`**: Emitted when a record enters a `RecordsFeed`.
- **`CONSUMED`**: Emitted when a `@feed_consumer` starts processing a record.
- **`LINKED`**: Manual association of a record to the current run.

All logs and spans emitted within a `@feed_consumer` (or where `current_record_key` is set) are automatically correlated to the record identity.

#### Logging

**File:** `observability/logging.py`

Public API: `from flowstash.observability.logging import logger`

The logger automatically enqueues `LogEvent`s to BigQuery/FileStore when running inside an integration context.

```python
from flowstash.observability.logging import logger

logger.info("Something happened", details="...", fw={"record_key": "custom_key"})
```

---

## Architecture

```
flowstash/
├── __init__.py
├── context.py          # IntegrationContext & context management
├── decorators.py       # @integration_step, @integration_task
├── pipelines/
│   ├── ingress.py      # @ingress.webhook, @ingress.poll
│   ├── records_feed.py # RecordsFeed publishing & dedupe
│   ├── records_model.py# RecordData model
│   └── consumer.py     # @feed_consumer decorator
├── clients/
│   ├── http.py         # HttpClient with auth & observability
│   └── odata.py        # ODataClient for OData services
├── config/             # Configuration models & loaders
├── queue/              # TaskBackend protocol & backends (Dramatiq, Asyncio)
├── observability/
│   ├── logging.py      # Correlated logger shim
│   ├── model.py        # Domain models (including RecordLink)
│   ├── ingestion.py    # Event recording API
│   ├── record_links.py # RecordLink specific logic
│   ├── registry.py     # Store registry
│   ├── schema.sql      # BigQuery schema
│   └── stores/         # GCS, BigQuery, File, PubSub stores
└── tests/              # Test suite
```
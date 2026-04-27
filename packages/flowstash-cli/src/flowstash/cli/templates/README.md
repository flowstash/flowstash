# {project_name}

flowstash application generated for {project_name}.

## Structure

- `api.py`: Entry point for the API service.
- `worker.py`: Entry point for the worker service.
- `src/shared/`: Shared helpers, constants, and client utilities.
- `src/api/`: Webhook handlers and API startup.
- `src/worker/`: Worker startup and task definitions (such as feed consumers).
- `config/`: Environment-based configuration (e.g. `local/`, `dev/`, `prod/`, `shared/`).
- `deployment/`: Docker Compose setup and Dockerfiles.

---

## Dependency Management

This project defines dependencies using optional extras, allowing you to install only what is needed for a specific service.

- **Base dependencies**: Required by both API and Worker.
- **`api` extra**: Dependencies required only by the API service.
- **`worker` extra**: Dependencies required only by the Worker service.
- **`dev` extra** (optional): Development tooling.

### Standard Installation (pip)

Install for local development (recommended, installs everything):
```bash
pip install -e ".[api,worker,dev]"
```

Install API-only:
```bash
pip install -e ".[api]"
```

Install Worker-only:
```bash
pip install -e ".[worker]"
```

### Alternative Installation (Poetry)

If you prefer managing your environment with Poetry:

Install for local development (everything):
```bash
poetry install --extras "api worker dev"
```

Install API-only:
```bash
poetry install --extras "api"
```

Install Worker-only:
```bash
poetry install --extras "worker"
```

---

## Running Locally

### 1. Start supporting services (e.g., Redis)
```bash
docker compose -f deployment/local/docker-compose.yaml up -d
```

### 2. Set environment
```bash
export BEAMSTACK_ENV=local
```

### 3. Run API
```bash
python api.py
```
API will be available at [http://localhost:8000](http://localhost:8000).

### 4. Run Worker
```bash
python worker.py
```

*(Alternatively, you can use the flowstash CLI to run your environments: `flowstash run local`)*

---

## flowstash Core Concepts Usage

This project is built on `flowstash` – a core library providing robust context management, queue abstractions, and observability. Here is how you can utilize its components in your application:

### Integration Context
`IntegrationContext` propagates automatically through all your integration operations. It manages the current `run_id`, `integration`, and `current_record_key` to ensure all logs and traces are correctly correlated without manual passing.

### Ingress & Pipelines
- **Webhook Ingress**: Use the `@ingress.webhook(pipeline="...", integration="...")` decorator on your FastAPI endpoints to attach integration context without manually wrapping your handlers.
- **Polling Ingress**: Use `@ingress.poll(pipeline="...", integration="...", schedule="...")` to create durable, stateful scheduled tasks (such as API polling).

### RecordsFeed & Consumers
Extract and ingest data efficiently using **RecordsFeed**:
```python
from flowstash.pipelines.records_feed import RecordsFeed, RecordData

feed = RecordsFeed.get(feed_id="your.pipeline")
feed.publish(RecordData(record_id="123", record_type="invoice", data={...}))
```
Records are automatically deduplicated and optionally ordered by timestamp. 

Consume them robustly with **Feed Consumers**, which handle batching, rate-limiting, and parallel execution automatically:
```python
from flowstash.pipelines.consumer import feed_consumer

@feed_consumer(
  feed_id="your.pipeline",
  batch=True,
  max_batch_size=100,
  rate_limit_per_sec=50,
)
async def process_records(records: list[RecordData]):
    for record in records:
        # Insert your business logic here
        pass
```

### Observability
Use the provided `logger` to naturally correlate your logs with the active context. The framework tracks the lifecycle of records intrinsically (from publishing to consumption) without manual `record_id` logging.

```python
from flowstash.observability.logging import logger

logger.info("Processing the invoice batch", details="step 1 completed")
```

# Quickstart

In five minutes you'll have a running integration: a webhook that publishes records to a feed, a consumer that processes them, and a scheduled task — all traced end to end. No Docker, no Redis; the development backend runs everything in-process.

## 1. Scaffold a project

```bash
pip install flowstash
mkdir hello-flowstash && cd hello-flowstash
flowstash init --name hello
```

The interactive setup asks for your first environment. Choose **dev**, backend **asyncio**, and observability **logfile**. You get a complete project:

```
hello-flowstash/
├── .flowstash               # project + environments manifest
├── api_main.py              # the API service (FastAPI)
├── worker_main.py           # the worker service
├── config/
│   ├── shared/              # base config: backend, clients, .env
│   └── dev/                 # dev overrides
├── deployment/              # Dockerfiles + docker-compose per env
└── src/
    ├── api/routes/          # webhooks & your own FastAPI routes
    ├── shared/              # clients, models, shared tasks
    └── worker/tasks/        # worker tasks
```

Install the app's dependencies:

```bash
pip install -e ".[api,worker,dev]"
```

## 2. Look at the generated webhook

`src/api/routes/webhooks.py` already contains a working ingress that publishes to a record feed:

```python
from flowstash import ingress
from flowstash.pipelines import RecordData
from flowstash.pipelines.records_feed import RecordsFeed
from fastapi import Request

@ingress.webhook(integration="demo", pipeline="process_user",
                 path="/demo/process_user", method="POST")
async def process_user_webhook(request: Request):
    data = await request.json()
    user_id = data.get("user_id")
    if not user_id:
        return {"error": "user_id required"}, 400
    feed = RecordsFeed.get("test_feed")
    await feed.publish(RecordData(record_id=user_id, record_type="user",
                                  data={"user_id": user_id}))
    return {"status": "submitted", "user_id": user_id}
```

## 3. Add a consumer

Create `src/worker/tasks/consumers.py`:

```python
from flowstash.pipelines import RecordData, feed_consumer
from flowstash.observability.logging import logger

@feed_consumer(feed_id="test_feed", subscription="greeter")
async def greet_user(record: RecordData):
    logger.info("processing user %s", record.record_id)
```

That's the whole pipeline: webhook → feed → consumer. The feed deduplicates by record id and always delivers the latest state.

## 4. Run it

```bash
export ENVIRONMENT=dev
python api_main.py
```

The API starts on port 8000. Webhooks are mounted under the `/webhooks` prefix, so send an event:

```bash
curl -X POST localhost:8000/webhooks/demo/process_user \
     -H 'Content-Type: application/json' \
     -d '{"user_id": "42"}'
```

You'll see the webhook accept the request and the consumer log `processing user 42` — with the asyncio backend, feed delivery happens in-process, so one command runs everything.

## 5. See the trace

You chose the `logfile` observability store, so every run is captured under `logs/observability/` — one directory per run containing the run event, spans, logs, and the data exchange of the incoming webhook, all correlated by `run_id`:

```bash
ls logs/observability/
```

## 6. What about the scheduled task?

`src/worker/tasks/tasks.py` defines a task with a cron schedule:

```python
@integration_task(integration="demo", integration_pipeline="demo",
                  default_schedule=Schedule(cron="0 0 * * 0"))
def demo_task():
    print("Processing user...")
```

Scheduled tasks and worker tasks execute in the worker process:

```bash
python worker_main.py     # in a second terminal
```

Or run both services (plus Redis, if the environment uses the Dramatiq backend) with Docker Compose in one step:

```bash
flowstash run dev
```

## Where next

- [Project Setup](project-setup.md) — the config tree, environments, and `.env` files explained
- [Building an Integration](../guides/building-an-integration.md) — the full walkthrough: clients, auth, polls, delivery
- [Architecture](../concepts/architecture.md) — how the pieces fit, and what changes in production

# Local Development

Three ways to run a project locally, from lightest to most production-like.

## Bare Python (asyncio backend)

With `backend.type: asyncio` there are no external dependencies — the queue, scheduler, and feed delivery run in-process:

```bash
pip install -e ".[api,worker,dev]"
export ENVIRONMENT=dev
python api_main.py
```

For most development this **one process is enough**: webhooks, feed consumers, and schedules all execute inside the API service (the asyncio lifespan runs an in-process scheduler; disable with `FLOWSTASH_ASYNC_SCHEDULED_ENABLE=false`). Run `python worker_main.py` separately only when you specifically want the worker path.

Two handy environment variables:

- `PORT` — API port (default 8000)
- `FLOWSTASH_STARTUP_TASK=<task name>` — submit one task at startup; convenient for iterating on a single task

## Docker Compose

```bash
flowstash run dev              # docker compose up --build
flowstash run dev -d --logs    # detached, following logs
flowstash run dev --no-build   # skip image rebuild
```

This runs `deployment/dev/docker-compose.yaml`: the API (port 8000), the worker, and — for environments created with the `dramatiq` backend — a Redis container with `REDIS_URL` pre-wired. It's the closest local approximation of the self-hosted production stack.

## VS Code

```bash
flowstash env init-vscode dev
```

adds `Launch dev API` and `Launch dev Worker` debug configurations (with `ENVIRONMENT` and `PYTHONPATH` set), so you can set breakpoints inside tasks and webhook handlers.

## The local development loop

```bash
flowstash client curl crm /contacts        # poke a configured API client, auth applied
flowstash webhook listen                   # capture real webhook payloads → fixtures
flowstash webhook test --path /shop/orders # replay a captured fixture at localhost
flowstash check                            # validate project structure
```

With `storeType: local` observability, each run's trace lands in `logs/observability/` — see [Monitoring & Debugging](../guides/monitoring-and-debugging.md).

## Gotchas

- **Delayed tasks** (`task.schedule(...)`) raise on the asyncio backend — use Docker Compose with the dramatiq backend when you need to exercise them.
- **Feed policies** (rate limits, concurrency, debounce) are not enforced locally — declared, synced to the platform, enforced there. Don't load-test them on your laptop.
- State on the asyncio profile lives in `.flowstash_state.db` (SQLite); delete it to reset cursors.

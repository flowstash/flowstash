# Dramatiq Backend

The self-hosted production backend: tasks flow through [Dramatiq](https://dramatiq.io) over Redis, feeds use Redis streams, and everything runs on infrastructure you operate.

## Configuration

```yaml
# config/prod/backend.yaml
backend:
  type: dramatiq
  dramatiq:
    redis_url: ${REDIS_URL}     # env var wins over this value anyway
state_store:
  type: redis
  redis:
    redis_url: ${REDIS_URL}
webhooks:
  prefix: /webhooks
```

`REDIS_URL` (default `redis://localhost:6379/0`) is the single knob — the task broker, feed streams, and state store all use it.

## Topology

```
external systems ──webhooks──▶ API service ──┐
                                             ├──▶ Redis ──▶ worker service(s)
            scheduler (in worker, APScheduler)┘
```

- **API service** (`python api_main.py`) — serves webhooks, submits tasks, publishes to feeds.
- **Worker service** (`python -u worker_main.py`) — runs the Dramatiq worker (a thread pool; async tasks get a dedicated event loop per thread), the feed consumer runner, **and the cron scheduler**. Start the worker with the framework's entry point, not the `dramatiq` CLI — the framework owns the broker setup.
- **Redis** — the only stateful piece. Feeds are Redis streams with compaction; losing Redis loses queued tasks, feed retention, and state, so persist it (AOF/RDB) and back it up like a database.

The scaffolded `deployment/<env>/docker-compose.yaml` wires exactly this: `api`, `worker`, and `redis:7-alpine`.

## Scaling

- **More task throughput:** run more worker replicas — Dramatiq consumer groups distribute work automatically.
- **Scheduler caveat:** every worker process runs the APScheduler. With multiple worker replicas, cron jobs fire once **per replica** — make scheduled tasks idempotent, dedicate a single scheduler replica, or route recurring work through polls whose feed dedup absorbs duplicates.
- **API replicas:** stateless; scale freely behind a load balancer.

## Operations

- **Retries & failures:** a task that raises follows Dramatiq's retry policy; feed records that fail remain pending in their consumer group for redelivery. Watch Redis stream pending counts (`XPENDING rf:<feed_id>:stream <subscription>`) for stuck consumers.
- **Graceful shutdown:** SIGTERM lets the worker finish in-flight messages and flush observability.
- **Observability:** pair with `storeType: managed` (platform ingestion) or `local`. Each executed message records a run with a fresh `run_id`, linked to its submitter.
- **Feed policies:** rate limiting/concurrency/debounce declared on consumers are not enforced by this backend — enforce rate limits at the client level (retry on 429 is built in) if the destination is sensitive.

## Deploying

Any container platform works. Build the two images and provide `REDIS_URL` and `ENVIRONMENT`:

```bash
docker compose -f deployment/prod/docker-compose.yaml build
# push to your registry, run under your orchestrator of choice
```

See [CI/CD](ci-cd.md) for pipeline examples.

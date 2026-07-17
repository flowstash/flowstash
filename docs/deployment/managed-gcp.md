# Managed Deployment

In `managed` mode, FlowStash runs your containers on Google Cloud Run while the **FlowStash platform** owns every stateful moving part: the task queue, schedules, feed storage and deduplication, delivery policies, idempotency leases, and observability. You write the same code as everywhere else; operations disappear.

## How it works

```
                       ┌────────────── FlowStash platform ──────────────┐
external ──webhooks──▶ │  Cloud Tasks · Cloud Scheduler · feed storage  │
                       │  dedup · leases · observability · builds       │
                       └───────┬───────────────────────────▲────────────┘
                               │ HTTP push                 │ submit / publish / register
                               ▼                           │
                    your Cloud Run services (API + worker, autoscaled)
```

- **Submitting work** — `task.submit()`, `feed.publish()`, and schedule declarations become calls to the platform API.
- **Receiving work** — the platform pushes back over HTTP: Cloud Tasks POSTs each task to your worker's `/handle_task`, and feed deliveries hit dedicated endpoints. A non-2xx response means Cloud Tasks retries — at-least-once delivery with backoff, no broker for you to run.
- **Schedules** — registered with the platform at deploy time; Cloud Scheduler fires them.
- **Scale-to-zero-safe** — because work arrives as HTTP, Cloud Run can scale your services to zero between events; the platform holds the state.

## Deploying

One-time setup:

```bash
flowstash login                 # browser-based auth
flowstash link                  # bind the project to a platform project
flowstash env add prod          # choose backend: managed
```

Then, for every release:

```bash
flowstash deploy                # = build + deploy to prod
```

`deploy` bundles your source, runs a **cloud build** (no local Docker needed), and rolls it out: validation → deploy → health check → schedule registration → done. The output shows your service's API URL and the registered schedules. Deploy a prebuilt artifact with `--artifact`, or build separately first with `flowstash build prod`.

The deployed services authenticate to the platform with a `MANAGED_AUTH_TOKEN` injected at deploy time; `ENVIRONMENT` selects the config layer, exactly as locally.

## Deployment profiles

Resource presets (CPU, memory, max instances for the worker and API) are chosen per environment:

```bash
flowstash deploy configure --env prod
```

Pick a profile interactively, and decide the cold-start trade-off:

- `--always-on` — keep one API instance warm (`min_api_instances: 1`); webhooks never pay a cold start.
- `--no-always-on` — scale to zero; cheapest, with cold-start latency on the first request.

Changes apply immediately with `--apply`, or on the next deploy.

## Idempotency leases

Cloud Tasks retries aggressively; the optional **lease broker** guarantees a run executes exactly once even under racing retries. The worker holds a WebSocket to the platform's lease broker and acquires a lease per `run_id` before executing:

- lease held elsewhere → respond 503, Cloud Tasks retries later
- run already completed → acknowledge as duplicate, skip

Enable with `LEASE_BROKER_ENABLED=true` on the worker service. Without it, rely on idempotent task design (recommended regardless).

## Feed delivery, managed

Consumer declarations (`batch`, `max_batch_size`, `debounce_delay_ms`, `rate_limit_per_sec`, `concurrency`) are synced to the platform at startup and deploy — and **enforced there**. Batched consumers lease a batch, process, and acknowledge per-item success/failure; failed items are redelivered. Deduplication is transactional on the platform side.

## Operational visibility

- `flowstash deploy` output — deploy status timeline and registered schedules
- The platform UI — runs, spans, logs, data exchanges, record links (with `storeType: managed` observability, which is the natural pairing)
- `GET /health` on each service — standard Cloud Run health checks

## Environment variables (managed services)

| Variable | Purpose |
|---|---|
| `MANAGED_AUTH_TOKEN` | platform API auth (injected at deploy) |
| `FLOWSTASH_API_URL` | platform base URL |
| `ENVIRONMENT` | config layer selection |
| `PORT` | HTTP port (Cloud Run sets it) |
| `LEASE_BROKER_ENABLED` | opt into exactly-once leases |
| `FLOWSTASH_RUN_ID` | stable run id across job retries (set by the platform) |

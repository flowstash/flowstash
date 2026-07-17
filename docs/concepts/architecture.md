# Architecture

## The packages

FlowStash ships as five packages that install into a single `flowstash.*` namespace:

```{mermaid}
graph BT
    clients["flowstash-clients<br/><small>API clients, transports, auth</small>"]
    lib["flowstash-lib<br/><small>decorators, context, feeds, state, observability</small>"]
    runtime["flowstash-runtime<br/><small>FastAPI app builder, workers, backends</small>"]
    cli["flowstash-cli<br/><small>the <code>flowstash</code> command</small>"]
    meta["flowstash<br/><small>meta package</small>"]
    lib --> clients
    runtime --> lib
    runtime --> clients
    cli --> runtime
    meta --> runtime
    meta --> cli
```

| Package | Import root | What it gives you |
|---|---|---|
| `flowstash-clients` | `flowstash.clients` | `HttpClient`, GraphQL/OData helpers, auth, the client registry |
| `flowstash-lib` | `flowstash.decorators`, `.context`, `.ingress`, `.pipelines`, `.integration`, `.observability`, `.config` | the programming model |
| `flowstash-runtime` | `flowstash.runtime` | `create_fastapi_app`, `initialize_runtime`, `run_worker`, backend implementations |
| `flowstash-cli` | `flowstash.cli` | the `flowstash` CLI: scaffolding, local run, build, deploy, auth |
| `flowstash` | — | convenience meta package installing everything |

Installing `flowstash` gets you all of it; libraries that only need to *call* an API can depend on `flowstash-clients` alone.

## Runtime topology

A FlowStash project runs as **two processes** built from the same codebase and configuration:

```{mermaid}
graph LR
    subgraph app ["Your project"]
        api["API service<br/><small>api_main.py — FastAPI</small>"]
        worker["Worker service<br/><small>worker_main.py</small>"]
    end
    ext["External systems"] -- "webhooks" --> api
    api -- "publish records / submit tasks" --> transport[("task & feed<br/>transport")]
    transport --> worker
    worker -- "client calls" --> ext
    sched["scheduler"] -.-> worker
```

- The **API service** (`create_fastapi_app`) serves webhook routes (under `/webhooks` by default), a `/health` endpoint, and any FastAPI routers of your own it discovers under `src/api/routes/`.
- The **worker service** (`initialize_runtime` + `run_worker`) executes tasks, scheduled jobs, and feed consumers.
- Both boot the same way: load the layered YAML config for the current `ENVIRONMENT`, initialize observability and the client registry, then **auto-import** your source directories — importing a module is what registers its `@integration_task`, `@ingress.webhook`, `@ingress.poll`, `@feed_consumer`, and `@client` declarations. A module that fails to import fails the boot, loudly and on purpose.

## Pluggable backends

The transport between the two processes — the task queue, the feed pipeline, the scheduler, the state store — is selected by one config key, `backend.type`:

| | `asyncio` | `dramatiq` | `managed` |
|---|---|---|---|
| Task queue | in-process | Dramatiq over Redis | platform API → Cloud Tasks |
| Feeds | in-process | Redis streams | platform API |
| Scheduler | APScheduler (in API) | APScheduler (in worker) | Cloud Scheduler via platform |
| State store | SQLite file | Redis | platform state service |
| Infrastructure | none | Redis | none (hosted) |
| Use for | local dev, tests | self-hosted production | managed production |

The programming model is identical across all three — `task.submit()`, `feed.publish()`, `@feed_consumer` don't change. Promotion from laptop to production is a configuration change, not a rewrite. See [Deployment](../deployment/overview.md) for the decision guide.

### Managed mode in one paragraph

In `managed` mode your containers run on Cloud Run, but all the *stateful* infrastructure — queueing, scheduling, feed storage, deduplication, leases, observability — lives in the FlowStash platform. The platform pushes work to your service over HTTP (`/handle_task`, feed delivery endpoints, retried by Cloud Tasks on non-2xx), and your service registers its schedules and feed consumers with the platform at deploy time. Details in [Managed GCP](../deployment/managed-gcp.md).

## Configuration

Configuration is a directory tree, layered by environment:

```
config/
├── shared/            # base: backend.yaml, clients.yaml, clients/*.yaml, .env
└── dev/               # per-environment overrides (deep-merged, env wins)
```

`load_config_dir("config", environment=os.getenv("ENVIRONMENT", "dev"))` merges the layers, loads `.env` files, substitutes `${ENV_VAR}` placeholders, and initializes the global registries. See the [Configuration reference](../reference/configuration.md).

## Related

- [Getting started](../getting-started/quickstart.md) — see the topology run on your machine
- [Deployment overview](../deployment/overview.md) — choosing a backend

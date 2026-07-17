# Deployment Overview

A FlowStash project deploys as two containers — the API service and the worker — plus whatever the chosen backend needs. The backend is the decision that shapes everything else.

## The decision

| | `asyncio` | `dramatiq` | `managed` |
|---|---|---|---|
| **What it is** | everything in-process | Dramatiq workers over Redis | FlowStash platform + Cloud Run |
| **Queue & scheduler** | in the API process | Redis + in-worker scheduler | Cloud Tasks + Cloud Scheduler (platform) |
| **Infrastructure you run** | none | Redis + your containers | none — containers are deployed for you |
| **Delayed tasks** | ✗ | ✓ | ✓ |
| **Feed policy enforcement** (rate, concurrency, debounce) | ✗ | partial | ✓ |
| **Scaling** | one process | add workers | autoscaling, scale-to-zero or always-on |
| **Best for** | development, tests | self-hosted production | production without ops |

Rules of thumb:

- **Developing?** `asyncio`. Zero dependencies, instant feedback. Not for production — no durability, no delayed execution.
- **Self-hosting, comfortable operating Redis and containers?** `dramatiq`. Everything runs on your infrastructure; the scaffolded Docker Compose stack is a complete starting point.
- **Want deploys, scheduling, retries, and observability handled?** `managed`. `flowstash deploy` builds in the cloud and ships to autoscaling infrastructure; the platform owns all stateful moving parts.

Backends are per-environment: `dev` on `asyncio`, `prod` on `managed` is a normal setup. The integration code is identical across all three — only `config/<env>/backend.yaml` differs.

## What each page covers

- [Local Development](local-development.md) — running without Docker, with Docker Compose, and in VS Code
- [Dramatiq Backend](dramatiq-backend.md) — Redis, workers, scaling, operations
- [Managed GCP](managed-gcp.md) — the platform architecture, deploying, profiles, leases
- [CI/CD](ci-cd.md) — building and deploying from pipelines

## The container model

All images build on a shared base (`flowstash/flowstash-base`) with the framework preinstalled. The scaffold generates:

- `deployment/shared/api.Dockerfile` — installs the `api` extra, drops `src/worker/`
- `deployment/shared/worker.Dockerfile` — installs the `worker` extra, drops `src/api/`
- `deployment/<env>/docker-compose.yaml` — the local/self-hosted stack

Managed deployments don't use these directly: `flowstash build` bundles your source and builds remotely against the platform's base image.

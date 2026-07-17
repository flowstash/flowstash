# Project Setup

This page explains the anatomy of a FlowStash project — what `flowstash init` generates and why.

## The project manifest: `.flowstash`

```yaml
project_name: hello
environments:
  - name: dev
    managed: false
    options:
      backend: asyncio
      observability: logfile
```

`.flowstash` names the project and lists its **environments**. Each environment records whether it is *managed* (deployed to the FlowStash platform) and the choices made when it was created (backend type, observability). Once you link the project to the platform (`flowstash link`), the manifest also stores the `project_id` and account binding.

Manage environments with the CLI:

```bash
flowstash env list
flowstash env add prod          # interactive: backend + observability choices
flowstash env del staging
flowstash env init-vscode dev   # VS Code launch configs for API + worker
```

## The two entry points

Every project has two services built from the same code and config:

- **`api_main.py`** — loads config, builds the FastAPI app with `create_fastapi_app`, auto-importing `src/api/routes/`. Serves webhooks (under `/webhooks`), `/health`, and any FastAPI `router` you define in your route modules.
- **`worker_main.py`** — loads config, calls `initialize_runtime` auto-importing `src/shared/tasks/` and `src/worker/tasks/`, then `run_worker`. Executes tasks, schedules, and feed consumers.

Auto-import is how declarations register: importing a module runs its `@integration_task`, `@ingress.webhook`, `@feed_consumer`, and `@client` decorators. Modules that fail to import fail the boot — deliberately, so broken code never deploys silently.

## The config tree

```
config/
├── shared/                  # applies to every environment
│   ├── backend.yaml         # backend + state_store + webhooks
│   ├── clients.yaml         # pointer to the clients/ directory
│   ├── clients/
│   │   └── demoClient.yaml  # one file per API client
│   └── .env
└── dev/                     # per-environment overrides (deep-merged, env wins)
    ├── backend.yaml
    ├── observability.yaml
    └── .env
```

At startup, `load_config_dir("config", environment=$ENVIRONMENT)` merges `shared/` with the selected environment's directory. The `ENVIRONMENT` variable (default `dev`) is the only switch — same code, different config.

Two conveniences to know:

- **`.env` files** in both layers are loaded automatically (environment layer wins; real environment variables win over both for protected keys like `FLOWSTASH_API_URL`).
- **`${VAR}` substitution** — any `${NAME}` inside a YAML file is replaced by the environment variable, so secrets never live in config files.

Full schema: [Configuration reference](../reference/configuration.md).

## Source layout

```
src/
├── api/routes/       # @ingress.webhook handlers + your own FastAPI routers
├── shared/
│   ├── clients/      # @client subclasses with typed API methods
│   ├── models/       # Pydantic models for your records
│   └── tasks/        # tasks importable by both API and worker
└── worker/tasks/     # tasks + consumers that run only in the worker
```

The split matters at build time: the API container drops `src/worker/`, the worker container drops `src/api/` — each service ships only the code it needs.

## Deployment files

```
deployment/
├── shared/
│   ├── api.Dockerfile       # FROM flowstash/flowstash-base
│   └── worker.Dockerfile
└── dev/
    └── docker-compose.yaml  # api + worker (+ redis for the dramatiq backend)
```

`flowstash run <env>` is a wrapper around `docker compose up` on the environment's compose file. Environments created with the `dramatiq` backend automatically include a Redis service with `REDIS_URL` wired in.

## Keeping the structure healthy

```bash
flowstash check        # verifies required files exist
flowstash init --fix   # regenerates anything missing
```

## Next

- [Quickstart](quickstart.md) if you skipped it
- [Building an Integration](../guides/building-an-integration.md)

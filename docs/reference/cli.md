# CLI Reference

The `flowstash` command (installed by `flowstash-cli`). Run any command with `--help` for full option details; `flowstash` alone prints the command list.

Credentials live in the OS keyring; CLI settings in `~/.flowstash/config.yaml`; project settings in the `.flowstash` file at the project root. The platform API URL defaults to the hosted platform and can be overridden with `FLOWSTASH_API_URL`.

## Project

### `flowstash init`

Scaffold a new project (or repair an existing one) in the current directory.

```bash
flowstash init [--name/-n NAME] [--fix] [--force]
```

Interactive: prompts for the project name and the first environment (backend + observability choices), offers VS Code launch configs and platform linking. `--fix` regenerates missing mandatory files; `--force` overwrites.

### `flowstash check`

Validate the project structure against the expected template; reports missing files and suggests `flowstash init --fix`.

### `flowstash link`

Bind the local project to a platform project (pick an existing one or create it). Writes `project_id`/`tenant_id` into `.flowstash`. Required before managed builds/deploys.

### `flowstash env`

```bash
flowstash env list
flowstash env add [NAME] [--force]      # interactive backend + observability setup
flowstash env del NAME [--yes/-y]
flowstash env init-vscode NAME [--force]
```

Environments live in `.flowstash`; `add` also scaffolds `config/<env>/` and `deployment/<env>/`.

## Run, build, deploy

### `flowstash run`

```bash
flowstash run [ENV=dev] [--build/--no-build] [--detach/-d] [--logs] [--yes/-y]
```

Runs `deployment/<env>/docker-compose.yaml` via `docker compose up` (build included by default). `-d --logs` detaches then follows logs. Requires Docker.

### `flowstash build`

```bash
flowstash build [ENV=dev] [--tag/-t TAG] [--user/-u EMAIL]
```

- **Non-managed env** → local `docker compose build`.
- **Managed env** → cloud build: bundles source (excluding `.git`, `.venv`, caches), uploads it, triggers the remote build, and polls until success. Prints the resulting `artifact_id`. Requires login + linked project; no local Docker needed.

### `flowstash deploy`

```bash
flowstash deploy [ENV=prod] [--artifact/-a ID] [--yes/-y] [--user/-u EMAIL]
```

Managed environments only. Without `--artifact`, builds first. Rolls out through validation → deploy → health check → schedule registration, then prints the service's API URL and the registered schedules (task + cron).

### `flowstash deploy configure`

```bash
flowstash deploy configure [--env/-e ENV] [--profile/-p NAME]
                           [--always-on/--no-always-on] [--apply/--no-apply] [--yes]
```

Set the environment's deployment profile (CPU/memory/max-instance presets for worker and API) and warm-instance behavior (`--always-on` keeps one API instance warm; otherwise scale-to-zero). `--apply` rolls it out immediately; otherwise it takes effect on the next deploy.

## Authentication & accounts

```bash
flowstash login [--username/-u EMAIL --password/-p PWD]   # browser flow if omitted
flowstash logout [--user/-u EMAIL]
flowstash whoami [--user/-u EMAIL]      # alias: logged-in
flowstash accounts                      # all logged-in accounts
flowstash version
```

Multiple accounts can be logged in at once; a project pins its account via `linked_user` in `.flowstash`. Resolution order for the active account: `--user` flag / `FLOWSTASH_USER` env var → project `linked_user` → last-used account. Use `FLOWSTASH_USER` in CI.

## API keys

Manage platform API keys (used mainly for observability ingestion from deployed apps).

```bash
flowstash api-keys new [--label/-l TEXT] [--scope/-s SCOPE] [--env/-e ENV]   # alias: create
flowstash api-keys list
flowstash api-keys revoke KEY_ID [--yes/-y]
```

Scopes: `observability:ingest` (write-only ingestion — what apps should use) and `admin` (full management — keep out of project files). The raw key is shown **once**; `--env prod` writes it to `config/prod/.env` as `FLOWSTASH_API_KEY`.

## Clients

Inspect and call the API clients defined in your config — auth applied automatically. Works offline from the platform (no login needed).

```bash
flowstash client list [--env/-e ENV]
flowstash client curl CLIENT_ID PATH [-X METHOD] [--env/-e ENV]
                      [-H "Name: Value"]... [-d RAW | --json JSON|@file]
                      [-q key=value]... [-v] [-o FILE]
```

Examples:

```bash
flowstash client curl demoClient /users
flowstash client curl demoClient /items -X POST --json '{"name": "x"}'
flowstash client curl demoClient /search -q "q=hello" -q "page=1" -v
```

## Webhook

Capture and replay webhook payloads.

```bash
flowstash webhook listen [--entry/-e api_main.py] [--debug]
flowstash webhook test [--entry/-e api_main.py] [--path PATH] [--target http://localhost:8000]
```

`listen` opens a temporary public ingest URL for a chosen webhook, streams captured deliveries live, saves a selected payload as a JSON fixture under `tests/payloads/webhooks/`, and offers to patch the decorator with `test_payload=FromFile(...)`. `test` replays a webhook's fixture against a running server. `listen` requires login and a linked project.

## Environment variables the CLI honors

| Variable | Effect |
|---|---|
| `FLOWSTASH_API_URL` | platform API base URL |
| `FLOWSTASH_USER` | account selection (equivalent to `--user`) |

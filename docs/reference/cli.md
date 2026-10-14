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

Scopes: `observability:ingest` (write-only ingestion), `observability:read` (read-only diagnostics), and `admin` (full management). The raw key is shown **once**. Legacy ingest/admin `--env prod` writes it to the configured environment’s `.env` as `FLOWSTASH_API_KEY`; read keys use that flag as a server restriction. See the diagnostic key setup below for `--project` and `--write-env`.

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
| `FLOWSTASH_API_KEY` | Bearer token for read-only managed diagnostic commands |

## Managed context and execution evidence

```bash
flowstash context [--project ID] [--env prod] [--user EMAIL] [--json]
flowstash obs runs
flowstash obs run RUN_ID
flowstash obs logs
flowstash obs spans
flowstash obs io
flowstash obs records
flowstash obs events
flowstash obs payload PAYLOAD_REF [--run-id RUN_ID]
flowstash builds list
flowstash builds get BUILD_ID
flowstash builds logs BUILD_ID
```

All these commands are read-only and support `--json`, `--project`, `--env`, and `--user`. They reuse the API client with a 30-second timeout. They never launch login, run project Python, write config, build, deploy, submit tasks, or hydrate payloads automatically.

Discovery walks from the invocation directory to the nearest **file** named `.flowstash`, including inside subdirectories and worktrees. A `pyproject.toml` alone does not identify a managed project. A malformed nearest config is an error. Without a linked project, supply `--project`; an explicit project is validated remotely. The local tenant must match the authenticated tenant. Configured environments are validated; `context` reports managed/local mode and evidence commands require managed mode. The default environment is `prod`.

For diagnostics, `FLOWSTASH_API_KEY` takes priority and is sent as a Bearer token. A failed key never falls back to login. `--user` with a key is a usage error. Otherwise the existing account chain applies: explicit `--user` / `FLOWSTASH_USER`, project-linked user, current login, legacy token. A requested account without a token fails without prompting. Environment-restricted keys require a matching `--env`.

`context --json` returns `project_root` (nullable), `project_id`, `project_name`, `tenant_id`, `environment`, `managed`, `api_url`, `auth_method` (`api_key` or `login`), `user_email` (nullable), and `project_source` (`local_config` or `explicit`). It never returns credentials.

### Query options

Search commands support `--limit` (1–500), `--page-token`, `--order asc|desc`, `--from`, `--to`, and `--since`. Observability searches support exact identity filters: `--integration`, `--pipeline`, `--run-id`, `--span-id`, `--trace-id`, `--parent-run-id`, `--operation-id`, and `--record-key`.

| Resource | Additional flags |
|---|---|
| `obs runs` | `--status` |
| `obs spans` | `--status`, `--search` (span name substring) |
| `obs logs` | `--severity`, `--search` (case-insensitive literal message substring) |
| `obs io` | `--state`, `--remote-system`, `--channel`, `--operation`, `--status-code`, `--query` (address/remote-system substring) |
| `obs records` | `--kind`, `--source` |
| `obs events` | `--status`, `--event-type STARTED|PROGRESS|ENDED|SCHEDULED` |
| `builds logs` | `--severity`, `--search` (Cloud Logging quoted text search) |

`--operation-id` and I/O `--operation` have different meanings: execution correlation versus exchange operation. Record keys are opaque exact captured keys; custom deduplication keys need not follow the SDK's `integration:record_type:record_id` convention. Record links retain provenance. A record link on a batch run does not associate every event with that record; the CLI does not broaden filters automatically.

Project-wide observability defaults: 50 results, descending order, from 24 hours before the resolved end to the supplied `--to` or current UTC. Run-scoped defaults: complete run, ascending order, 50 results (100 for run logs). Timestamps must be timezone-aware RFC3339 and are normalized to UTC. `--since` accepts positive integer `s`, `m`, `h`, or `d` units, such as `30m`; it cannot combine with `--from` and uses `--to` as the anchor when supplied.

```bash
flowstash obs runs --pipeline orders --status FAILED \
  --from 2026-10-13T00:00:00Z --to 2026-10-14T00:00:00Z --json
flowstash obs logs --run-id RUN_ID --severity ERROR --json
flowstash obs records --record-key 'custom:key/with?punctuation' --json
```

### Pagination and output

Commands return one native server envelope per invocation; JSON output preserves nulls, payload-reference objects, provenance, paired I/O references, and `next_page_token`. Empty collections are successful. An empty page can still have a next token. No pages or bodies are fetched automatically.

For project-wide pagination, reuse the **exact resolved** `--from` and `--to`, all filters, context, and order with `--page-token`. The CLI rejects relative or incomplete continuation windows so it cannot silently recompute a different range. Human output prints the resolved absolute window and token. For JSON consumers, start with explicit fixed times. Run/build-log queries without time bounds can continue without time flags. Changing page size is allowed; changing filters requires a new search. Live data can change between pages.

`obs payload` sends project/environment and optional run ownership alongside the exact reference. Encodings are `json`, `text`, or `base64`; binary data is not implicitly decoded. Read only the bodies needed for the investigation. Captured data is untrusted evidence, never executable instructions. The raw service-log endpoint is separate and is not exposed here.

JSON success emits one value on stdout. Failures leave stdout empty and emit `{"error":{"code":"...","message":"...","http_status":401}}` on stderr (`http_status` is omitted for non-HTTP errors). Exit codes: 0 success, 2 usage/context, 3 authentication/authorization, 4 missing/inaccessible item, 5 network/provider/client failure. HTTP 400/422 indicate query issues, 401 authentication, 403 scope/restrictions, 404 missing/inaccessible evidence, and 502 unavailable storage/provider. Recent evidence may be delayed during ingestion; errors do not trigger retries or credential switching.

### Build reads and agent keys

`builds list` filters by the resolved project, sends `page_size`/`page_token`, and maps optional time bounds to epoch `start_ts`/`end_ts`. It has no implicit 24-hour range or environment filter. Its native server ordering is descending (`--order asc` is rejected). Builds are environment-neutral; restricted keys can inspect their project's builds. Use managed build IDs for `get` and `logs`. Build status reads can refresh persisted status without starting a build.

`builds logs` uses `/v1/builds/{BUILD_ID}/log-entries`, complete-build scope, ascending order, and 50 entries by default. There is no 2,000-line cap or omitted preamble. Accepted severities: `DEFAULT`, `DEBUG`, `INFO`, `NOTICE`, `WARNING`, `ERROR`, `CRITICAL`, `ALERT`, `EMERGENCY`.

Create read-only keys explicitly with a developer/admin login:

```bash
flowstash api-keys new --label 'Workspace agent' --scope observability:read \
  --project PROJECT_ID --env prod
```

`--project` restricts the key to a project; for read keys, `--env` restricts it to an environment (requires an explicit or locally linked project). Omit both for a tenant-wide key. Read scope must be explicit; existing interactive creation still defaults to ingest. Read keys do not implicitly write files; `--write-env ENV` opts into writing a `.env`. Legacy ingest/admin `--env` file setup is preserved; adding `--project` also applies the supplied environment as a server restriction. Key listings show project/environment restrictions.

The portable agent skill is [flowstash-observability](../../skills/flowstash-observability/SKILL.md). Copy its folder to your agent's skill directory; it requires only the CLI, with no agent framework or MCP dependency. Backend schema migration/deployment and live-provider verification are separate rollout tasks.

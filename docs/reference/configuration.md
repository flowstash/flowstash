# Configuration Reference

The complete schema of the `config/` directory and the environment variables the framework reads.

## Loading model

```python
from flowstash.config.env_loader import load_config_dir
config = load_config_dir("config", environment=os.getenv("ENVIRONMENT", "dev"))
```

- `config/shared/` is loaded first, then `config/<environment>/` is deep-merged on top (environment wins).
- `.env` files in both layers are loaded into the process environment (environment layer wins; real env vars win for protected keys `FLOWSTASH_API_URL`, `FLOWSTASH_API_KEY`).
- Any `${VAR}` inside a YAML value is substituted from the environment.
- YAML keys accept both snake_case and camelCase aliases where noted.
- `ENVIRONMENT=SMOKE-TEST` short-circuits to a minimal config — useful for CI boot checks.

## `backend.yaml`

```yaml
backend:
  type: dramatiq            # asyncio | dramatiq | managed   (default: dramatiq)
  dramatiq:
    redis_url: ${REDIS_URL} # optional; REDIS_URL env var takes precedence

webhooks:
  prefix: /webhooks         # mount point for @ingress.webhook routes

state_store:
  type: redis               # redis | sqlite | managed       (default: redis)
  sqlite:
    db_path: .flowstash_state.db
  redis:
    redis_url: ${REDIS_URL}
  managed:
    base_url: null          # falls back to FLOWSTASH_API_URL
    api_key: null
```

| Backend type | Task queue | Feeds | Scheduler | Notes |
|---|---|---|---|---|
| `asyncio` | in-process | in-process | APScheduler in API | dev only; `MANAGED_AUTH_TOKEN` not needed |
| `dramatiq` | Dramatiq/Redis | Redis streams | APScheduler in worker | needs `REDIS_URL` |
| `managed` | platform | platform | platform | needs `MANAGED_AUTH_TOKEN` |

## `observability.yaml`

```yaml
durability: eventual        # eventual | immediate
storeType: managed          # disabled | console | local | managed   (default: disabled)
localStorePath: logs        # for storeType: local
managedApiUrl: null         # falls back to FLOWSTASH_API_URL
managedApiKey: ${FLOWSTASH_API_KEY}
projectId: null             # falls back to MANAGED_PROJECT_ID / FLOWSTASH_PROJECT_ID
flushOnTaskExit: true
logging:
  enabled: true
  minLevel: INFO
  includePrefixes: []       # logger-name filters
  excludePrefixes: []
  passthrough: true         # also mirror captured logs to stdlib logging
```

## `clients.yaml` and `clients/*.yaml`

`clients.yaml` points at a directory of per-client files:

```yaml
path: clients
pattern: "*.yaml"
recursive: false
```

Each client file is one `ClientSettings`:

```yaml
client_id: crm              # must match @client("...") if a subclass exists
baseUrl: https://api.example.com/v2      # required
timeout: 10.0               # seconds
handleRedirects: false
extra:                      # free-form; headers/params/cookies applied to requests
  headers:
    User-Agent: MyApp/1.0

auth:                       # optional; one of:
  type: none
  # --- basic ---
  # type: basic
  # username: ...
  # password: ...
  # --- api key ---
  # type: api_key
  # key: X-Api-Key
  # value: ${MY_KEY}
  # in: header              # header | query
  # --- oauth2 ---
  # type: oauth2
  # grantType: client_credentials       # or password / refresh_token (inferred)
  # client_id: ${ID}
  # client_secret: ${SECRET}
  # token_url: https://auth.example.com/token
  # username: ...           # presence switches to password grant
  # password: ...
  # refresh_token: ...      # used automatically when set
  # scopes: [read]
  # extra_params: {}
  # clientAuthMethod: client_secret_basic   # or client_secret_post

retry:
  maxRetries: 0             # retries are opt-in
  maxWait: 60.0             # backoff cap, seconds
  whitelist: []             # body keywords that force retry
  blacklist: []             # body keywords that forbid retry (wins)

tls:
  certFile: null            # mTLS client cert
  keyFile: null
  caBundle: null
  verifySSL: true

suppress:                   # traffic governance rules
  - path: "/items/**"       # glob: * one segment, ** recursive
    POST:                   # HTTP method, or "*"
      behaviour: mock       # allow | raise | mock
      mock-response:
        status: 200
        headers: {}
        content: '{"id": 1}'
        # file-ref: fixtures/item.json
```

Retryable statuses: `429, 500, 502, 503, 504` (plus connection/timeout errors). A `401` with OAuth2 invalidates the cached token and retries once with a fresh one. `Retry-After` is honored on 429.

## Environment variables

### Core

| Variable | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `dev` | which config layer to load |
| `PORT` | `8000` (API) / `8080` (managed worker) | HTTP port |
| `REDIS_URL` | `redis://localhost:6379/0` | Dramatiq broker, feed streams, Redis state store |

### Platform / managed

| Variable | Purpose |
|---|---|
| `MANAGED_AUTH_TOKEN` | platform auth token — **required** for `backend.type: managed` |
| `FLOWSTASH_API_URL` / `MANAGED_API_URL` | platform API base URL |
| `FLOWSTASH_API_KEY` | observability ingestion key (also feed-consumer auth alias) |
| `MANAGED_PROJECT_ID` / `FLOWSTASH_PROJECT_ID` | project id for managed stores |
| `SERVICE_URL` | worker callback URL for task pushes |
| `FLOWSTASH_RUN_ID` | stable run id across managed job retries |
| `LEASE_BROKER_ENABLED` / `LEASE_BROKER_URL` | exactly-once lease guard |

### Behavior toggles

| Variable | Default | Purpose |
|---|---|---|
| `FLOWSTASH_ASYNC_SCHEDULED_ENABLE` | `true` | run schedules in the asyncio API process |
| `FLOWSTASH_STARTUP_TASK` | — | submit one named task at startup (asyncio) |
| `FLOWSTASH_LOG_PASSTHROUGH` | config | override log passthrough per process |
| `FLOWSTASH_USER` | — | CLI account selection |

### Observability tuning

| Variable | Default | Purpose |
|---|---|---|
| `FLOWSTASH_OBS_BATCH_SIZE` | 100 | ingestion batch size |
| `FLOWSTASH_OBS_FLUSH_INTERVAL_MS` | 250 | background flush interval |
| `FLOWSTASH_OBS_RETRY_MAX_ATTEMPTS` | 5 | retries for regular events |
| `FLOWSTASH_OBS_RETRY_TERMINAL_ATTEMPTS` | 100 | retries for run-completion events |
| `FLOWSTASH_OBS_RETRY_BACKOFF_BASE_MS` | 100 | retry backoff base |

## `.flowstash` (project manifest)

```yaml
project_name: myproject
project_id: null            # set by `flowstash link`
tenant_id: null
linked_user: null           # account pinned by `flowstash login`
environments:
  - name: dev
    managed: false
    options:                # choices made at env creation
      backend: asyncio
      observability: logfile
```

Managed by the CLI (`init`, `env`, `link`, `login`) — edit by hand only when you know why.

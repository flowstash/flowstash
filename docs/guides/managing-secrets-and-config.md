# Secrets & Configuration

How configuration is layered, where secrets live, and how to keep environments (dev, staging, prod) cleanly separated.

## The layering model

At boot, `load_config_dir("config", environment=$ENVIRONMENT)` builds the effective config from two layers:

```
config/shared/     ← base (all environments)
config/<env>/      ← overrides for the selected environment (deep-merged, wins)
```

`ENVIRONMENT` (default `dev`) is the only switch. Files recognized in each layer: `backend.yaml`, `observability.yaml`, `clients.yaml` (+ the `clients/` directory it points to), and `.env`.

Typical split:

- **`shared/`** — everything structural: client definitions, webhook prefix, retry policies.
- **`<env>/`** — what genuinely differs: backend type, observability store, base URLs for sandboxes, and each environment's `.env`.

## Secrets: `${VAR}` + `.env`

Config files never contain secrets. They contain **references**:

```yaml
# config/shared/clients/crm.yaml
auth:
  type: oauth2
  client_id: "${CRM_CLIENT_ID}"
  client_secret: "${CRM_CLIENT_SECRET}"
```

Values come from the process environment, populated from `.env` files at load time:

```bash
# config/dev/.env  (gitignored!)
CRM_CLIENT_ID=dev-client
CRM_CLIENT_SECRET=dev-secret
```

Precedence, lowest to highest: `config/shared/.env` → `config/<env>/.env` → the real process environment (protected keys such as `FLOWSTASH_API_URL` and `FLOWSTASH_API_KEY` always win from the real environment, so CI/production injection can't be shadowed by a stale `.env`).

:::{admonition} Gitignore your .env files
:class: danger
The scaffold creates `.env` placeholders; make sure they're in `.gitignore` before the first real secret lands. In managed deployments, secrets are injected as environment variables on the service — `.env` files are a local-development convenience.
:::

## Environment-specific overrides

Point dev at a sandbox without touching the shared definition — same `client_id`, only the changed keys:

```yaml
# config/dev/clients/crm.yaml
client_id: crm
baseUrl: https://sandbox.example-crm.com/v2
```

Or block writes entirely in dev with a [suppression rule](calling-external-apis.md#mock-or-block-endpoints).

## Adding a new environment

```bash
flowstash env add staging
```

The interactive setup asks for the backend (`asyncio` / `dramatiq` / `managed`) and observability store, then scaffolds `config/staging/` and `deployment/staging/`. Check the result into git (minus `.env`).

## Reading config at runtime

Occasionally an integration needs its own settings. Two idioms:

- Put them under a client's `extra:` block if they belong to that API.
- Read environment variables directly (`os.getenv`) for standalone knobs, documenting them in the environment's `.env` template.

## Platform API keys

Managed observability and deployment authenticate with platform API keys:

```bash
flowstash api-keys new --label "prod worker" --scope observability:ingest --env prod
```

`--env prod` writes `FLOWSTASH_API_KEY=...` into `config/prod/.env` for you. Use the narrow `observability:ingest` scope for anything that ships with the app; keep `admin` keys out of project files entirely. Rotate with `flowstash api-keys list` / `revoke`.

## Related

- [Configuration reference](../reference/configuration.md) — every key
- [Calling External APIs](calling-external-apis.md) — client auth schemes

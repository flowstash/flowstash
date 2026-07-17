# Calling External APIs

This guide covers the practical recipes for talking to external systems from your tasks: configuring auth, tuning retries, handling errors, and mocking endpoints. For the mental model, read [Clients](../concepts/clients.md) first.

## Define the client

Create one YAML file per client under `config/shared/clients/`:

```yaml
# config/shared/clients/crm.yaml
client_id: crm
baseUrl: https://api.example-crm.com/v2
timeout: 30
```

Environment-specific overrides go in `config/<env>/clients/` — for example a sandbox base URL in `config/dev/clients/crm.yaml`. Shared and environment layers are merged at load time.

## Choose an auth scheme

::::{tab-set}
:::{tab-item} API key
```yaml
auth:
  type: api_key
  key: X-Api-Key          # header (or query param) name
  value: "${CRM_API_KEY}"
  in: header              # or: query
```
:::
:::{tab-item} OAuth2 client credentials
```yaml
auth:
  type: oauth2
  client_id: "${CRM_CLIENT_ID}"
  client_secret: "${CRM_CLIENT_SECRET}"
  token_url: https://auth.example-crm.com/oauth/token
  scopes: [read, write]
  clientAuthMethod: client_secret_basic   # or client_secret_post
```

Tokens are cached and refreshed automatically (30-second expiry buffer); a `401` invalidates the cached token and the request is retried with a fresh one.
:::
:::{tab-item} OAuth2 password / refresh token
```yaml
auth:
  type: oauth2
  token_url: https://auth.example-crm.com/oauth/token
  client_id: "${CRM_CLIENT_ID}"
  client_secret: "${CRM_CLIENT_SECRET}"
  username: "${CRM_USER}"       # presence of username/password
  password: "${CRM_PASSWORD}"   # switches to the password grant
  # refresh_token: "${CRM_REFRESH_TOKEN}"   # used automatically when set
```
:::
:::{tab-item} Basic
```yaml
auth:
  type: basic
  username: "${CRM_USER}"
  password: "${CRM_PASSWORD}"
```
:::
::::

`${ENV_VAR}` placeholders are substituted from the environment (or your `.env` file) at config load time — secrets never live in the YAML itself.

## Make requests

```python
from flowstash.clients import get_client

crm = get_client("crm")

# GET with query params
resp = await crm.request("GET", "/contacts", params={"page": "1"})
contacts = resp.json()

# POST JSON
resp = await crm.request("POST", "/contacts", json={"email": "a@b.co"})

# Custom headers, per-request timeout
resp = await crm.request("GET", "/reports/big", headers={"Accept": "text/csv"}, timeout=120)
```

An error response raises `httpx.HTTPStatusError` with the method, path, and response body embedded, and logs a masked, copy-pasteable `curl -v` command reproducing the request.

## Tune retries

```yaml
retry:
  maxRetries: 3        # default 0 — retries are opt-in
  maxWait: 60          # cap on backoff seconds
  whitelist: []        # response-body keywords that force a retry
  blacklist: []        # response-body keywords that forbid a retry (wins)
```

With retries enabled, the client retries `429/500/502/503/504` and connection/timeout errors, backing off exponentially with jitter and honoring `Retry-After` on 429.

## Add typed methods

For anything beyond a couple of ad-hoc calls, wrap the API in a subclass so tasks read cleanly:

```python
# src/shared/clients/crm.py
from flowstash.clients import HttpClient, client

@client("crm")
class CrmClient(HttpClient):
    async def contacts(self, page: int = 1) -> list[dict]:
        resp = await self.request("GET", "/contacts", params={"page": str(page)})
        return resp.json()["items"]

    async def upsert_contact(self, payload: dict) -> dict:
        resp = await self.request("POST", "/contacts", json=payload)
        return resp.json()
```

You can also override `mask_sensitive_data()` in the subclass to redact API-specific fields from observability events.

## Mock or block endpoints

`suppress` rules intercept requests by path glob and method — handy for dry runs and local development against production config:

```yaml
suppress:
  - path: "/contacts/**"
    POST:
      behaviour: mock          # or: raise, allow
      mock-response:
        status: 200
        content: '{"id": 1, "email": "mocked@example.com"}'
```

Path patterns support `*` (one segment) and `**` (recursive). `raise` turns writes into hard errors; `mock` returns the canned response without touching the network.

## Test from the command line

Before writing any task code, verify the client works — auth included:

```bash
flowstash client list                      # what's configured, per environment
flowstash client curl crm /contacts        # GET through the configured client
flowstash client curl crm /contacts -X POST --json '{"email":"a@b.co"}'
flowstash client curl crm /search -q "q=hello" -q "page=1" -v
```

## Related

- [Clients](../concepts/clients.md) — the concept page, including GraphQL and OData helpers
- [Secrets & Configuration](managing-secrets-and-config.md)
- [Testing Integrations](testing-integrations.md)

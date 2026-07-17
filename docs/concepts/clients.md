# Clients

Clients are how FlowStash integrations talk to external systems. A client bundles a base URL, authentication, retries, TLS, and observability into a single named object that your tasks look up at runtime — so integration code never hard-codes URLs or credentials.

## Two ways to define a client

### Config-only (no code)

Declare the client purely in YAML. At runtime you get a plain `HttpClient` with auth and retries applied:

```yaml
# config/shared/clients/demoClient.yaml
client_id: demoClient
baseUrl: https://jsonplaceholder.typicode.com
timeout: 10
auth:
  type: api_key
  key: X-Api-Key
  value: "${DEMO_API_KEY}"
```

### Custom subclass (code + config)

Subclass `HttpClient` and register it with the `@client` decorator to attach typed domain methods:

```python
from flowstash.clients import HttpClient, client

@client("demoClient")
class DemoClient(HttpClient):
    async def get_users(self) -> list[dict]:
        resp = await self.request("GET", "/users")
        return resp.json()
```

The decorator name **must match the `client_id`** in the YAML — that's the link between code and config. When the registry instantiates `demoClient`, it uses your subclass with the settings from the YAML file.

## Using clients in tasks

```python
from flowstash.clients import get_client

client = get_client("demoClient")          # untyped lookup
users = await client.get_users()
```

or, if you defined a subclass:

```python
client = DemoClient.get_client()           # typed lookup
```

All requests go through a single async entry point:

```python
resp = await client.request(
    "POST", "/items",
    json={"name": "example"},
    params={"dryRun": "true"},
)
data = resp.json()
```

`request()` returns a raw `httpx.Response`. There are no `.get()`/`.post()` shortcuts — the explicit method string keeps call sites uniform.

## What the client does for you

- **Auth** — applied automatically per request. Supported schemes: `none`, `basic`, `api_key` (header or query), and `oauth2` (client credentials, password, and refresh-token grants, with token caching, a 30-second expiry buffer, and automatic re-auth on 401). See [Calling External APIs](../guides/calling-external-apis.md).
- **Retries** — opt-in via `retry.maxRetries`. Retries HTTP `429/500/502/503/504` and connection/timeout errors with exponential backoff and jitter, honoring `Retry-After` on 429. `whitelist`/`blacklist` keyword matching against the response body can fine-tune what is retried.
- **Observability** — every request emits a *data exchange event* into the run's trace, with `Authorization` headers, cookies, API keys, and secret-looking payload fields masked. On failure, a masked, reproducible `curl -v` command is logged for debugging.
- **Traffic governance** — `suppress` rules can intercept requests by path glob and method: allow them, raise, or return a mock response. Useful for dry runs and tests.
- **TLS** — custom CA bundles and mutual TLS via the `tls` config block.

## GraphQL and OData

For GraphQL and OData APIs, wrap the configured `HttpClient` in a protocol helper:

```python
from flowstash.clients import get_client
from flowstash.clients.graphql import GraphQLClient
from flowstash.clients.odata import ODataClient, ODataQuery

gql = GraphQLClient(get_client("myApi"), endpoint_path="/graphql")
data = await gql.query("query($id:ID!){ user(id:$id){ name } }", {"id": "1"})

od = ODataClient(get_client("myErp"))
q = ODataQuery().select("Name", "Id").filter("Active eq true").top(50)
async for entity in od.iter("Products", q):   # follows @odata.nextLink automatically
    ...
```

Both inherit the underlying client's auth, retries, and observability, since every call goes through `HttpClient.request()`.

## The registry lifecycle

1. Configuration loading reads `config/shared/clients.yaml` (a pointer to a directory of per-client YAML files), validates each file into `ClientSettings`, merges the shared and environment layers, and initializes the client registry.
2. Importing your client modules fires the `@client` decorators, registering subclasses (registrations that happen before config load are deferred and flushed on init).
3. `get_client("name")` instantiates the client on first use and caches it. Unknown names raise a `KeyError` listing the available clients.

## Related

- Guide: [Calling External APIs](../guides/calling-external-apis.md) — auth recipes, retries, mocking
- Guide: [Secrets & Configuration](../guides/managing-secrets-and-config.md) — `${ENV_VAR}` substitution, environments
- Reference: [Configuration](../reference/configuration.md) · [flowstash-clients API](../reference/api/flowstash-clients.md)
- CLI: `flowstash client list` and `flowstash client curl` — see the [CLI reference](../reference/cli.md)

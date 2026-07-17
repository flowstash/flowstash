# State

Integrations are long-lived: they need to remember cursors, sync watermarks, and checkpoints between runs. FlowStash provides a small, scoped key-value **state store** for exactly this.

```python
from flowstash.integration import State

State.set("cursor", {"page": 3}, scope="pipeline", ttl_s=3600)
cursor = State.get("cursor", scope="pipeline")    # {"page": 3}, or None if absent/expired
```

`State` resolves the current [execution context](context.md) automatically — call it from anywhere inside a run.

## Scopes

The scope determines the namespace a key lives in, derived from the current context:

| Scope | Namespace | Use for |
|---|---|---|
| `"integration"` (default) | per integration | settings shared across the integration's pipelines |
| `"pipeline"` | per pipeline | sync cursors, watermarks for one pipeline |
| `"ingress"` | per poll ingress | the injected poll state (managed for you) |

Requesting a scope whose context field isn't set (e.g. `pipeline` scope in a task with no pipeline) raises `RuntimeError` — as does using `State` outside a run entirely. In tests, bind a context explicitly:

```python
with State.use(ctx):
    State.set("k", "v")
```

## Semantics

- Values are JSON-encoded by default; `get` returns the decoded object.
- `ttl_s=None` means no expiry (and clears any previous TTL); an integer expires the entry after that many seconds. Expired entries read as `None`.
- Entries carry `updated_at` and an incrementing `version` — `State.get_entry(key)` returns the full `StateEntry` if you need the metadata.

## Poll state: the managed special case

[`@ingress.poll`](scheduling-and-ingress.md) handlers don't call `State` directly — the framework injects a `state: dict` as the first argument, loaded from the `ingress` scope, and persists it **only when the handler returns successfully**:

```python
@ingress.poll(integration="shopify", pipeline="orders", schedule="*/5 * * * *")
async def poll_orders(state: dict):
    since = state.get("cursor")
    orders = await fetch_orders(since)
    ...
    state["cursor"] = orders[-1]["updated_at"]   # persisted on success only
```

A failing run leaves the cursor untouched, so the next attempt re-fetches the same window — at-least-once, never lost progress.

## Backends

Configured via the `state_store` block in `backend.yaml`:

```yaml
state_store:
  type: redis        # redis | sqlite | managed
  redis:
    redis_url: ${REDIS_URL}
```

| Type | Storage | When |
|---|---|---|
| `sqlite` | local file (`.flowstash_state.db`) | local development, single process |
| `redis` | Redis | Dramatiq deployments |
| `managed` | platform state service (HTTP) | managed deployments |

When no store is configured at all (bare scripts, unit tests), FlowStash falls back to a shared **in-memory SQLite** store — convenient, but process-local and gone on exit.

## Related

- [Scheduling & Ingress](scheduling-and-ingress.md) — poll state injection
- [Configuration reference](../reference/configuration.md)

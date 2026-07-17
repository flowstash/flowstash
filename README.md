# FlowStash

**Think Vercel for integrations and background jobs.**

Write the integration, configure the environment, and deploy it — without designing the surrounding runtime from scratch. Webhooks, polling, queues, scheduled jobs, API clients, and observability are part of the framework's execution model, not separate infrastructure you have to assemble and connect yourself.

> 📚 **[Full documentation](https://flowstash.github.io/flowstash/)** · [Quickstart](https://flowstash.github.io/flowstash/getting-started/quickstart/) · [Architecture](https://flowstash.github.io/flowstash/concepts/architecture/)

## Why should I care?

Every integration project starts the same way: a webhook receiver, a queue, a scheduler, retry logic, an HTTP client with auth, some logging you'll regret later. You end up designing a small distributed system before writing a single line of business logic — and then designing it again for the next project.

FlowStash collapses all of that into a standard runtime:

- **Integration code stays integration logic.** You write how data is fetched, transformed, and delivered. The framework handles execution, scheduling, queues, retries, context propagation, and telemetry.
- **Webhooks, polling, queues, and scheduled jobs are first-class concepts** — declared with decorators, not assembled from infrastructure.
- **Two simple services, always the same shape.** An **API** service receives webhooks and inbound requests; a **worker** executes queued tasks, scheduled jobs, retries, and polling. You never redesign this topology per project.
- **Third-party APIs are described and accessed consistently.** Authentication, base URLs, retries, timeouts, and observability use the same client infrastructure across every integration — an OAuth2 API and an API-key API are the same one YAML file apart.
- **Local and production execution use the same code.** Run tasks directly while developing, submit them to a queue in production, or put them on a schedule — without rewriting the integration.
- **Deployment is intentionally simple.** Every integration has the same structure and runtime, so there's no new deployment model to invent. Use the managed platform (`flowstash deploy`) or host it yourself on infrastructure — no application code changes either way, just change the backend type in config.
- **Observability is part of the execution model.** Runs, tasks, HTTP calls, payloads, errors, and retries are correlated automatically instead of being reconstructed from scattered logs.

The result: integrations stop being bespoke glue and become **portable, repeatable software units**.

## What it looks like

```python
from flowstash import ingress
from flowstash.pipelines import RecordsFeed, RecordData, feed_consumer
from flowstash.clients import get_client
from fastapi import Request

# Receive: an inbound webhook publishes into a deduplicated record feed
@ingress.webhook(integration="shop", pipeline="orders", path="/shop/orders")
async def order_webhook(request: Request):
    order = await request.json()
    await RecordsFeed.get("orders").publish(
        RecordData(record_id=order["id"], record_type="order", data=order)
    )
    return {"received": True}

# Deliver: a consumer syncs each record — retried, debounced, fully traced
@feed_consumer(feed_id="orders", subscription="erp-sync", debounce_delay_ms=2000)
async def sync_to_erp(record: RecordData):
    erp = get_client("erp")           # OAuth2, retries, masking — from YAML config
    await erp.request("POST", "/orders", json=record.data)
```

That's a complete pipeline: an inbound webhook, a compacted latest-wins feed, and a delivery step with credential management, retries, and end-to-end tracing — none of which you wrote.

## Quickstart

```bash
pip install flowstash
flowstash init --name hello
pip install -e ".[api,worker,dev]"
python api_main.py
curl -X POST localhost:8000/webhooks/demo/process_user -d '{"user_id": "42"}' \
     -H 'Content-Type: application/json'
```

Continue with the [five-minute quickstart](https://flowstash.github.io/flowstash/getting-started/quickstart/).

## Packages

This monorepo publishes five packages sharing the `flowstash.*` namespace:

| Package | Role |
|---|---|
| [`flowstash`](packages/flowstash) | meta package — installs everything |
| [`flowstash-lib`](packages/flowstash_lib) | the programming model: decorators, context, feeds, state, observability |
| [`flowstash-clients`](packages/flowstash_clients) | API clients: HTTP, GraphQL, OData, auth |
| [`flowstash-runtime`](packages/flowstash_runtime) | the engine: FastAPI app builder, workers, backends |
| [`flowstash-cli`](packages/flowstash-cli) | the `flowstash` command |

Dependency graph: `clients` ← `lib` ← `runtime` ← `cli`; `flowstash` (meta) installs `runtime` + `cli`.

## Development

Requires Python ≥ 3.11. The repo is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/); local path dependencies are pre-wired.

```bash
uv sync          # install all workspace packages
make test        # run tests
make lint        # ruff check
make format      # ruff format
```

Docs live in `docs/` (Sphinx + Furo), managed as a uv dependency group: `uv sync --group docs && uv run sphinx-autobuild docs docs/_build/html`.

See [Contributing](https://flowstash.github.io/flowstash/community/contributing/) for the full workflow and the release process.

## License

[MIT](LICENSE)

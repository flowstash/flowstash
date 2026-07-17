# FlowStash

```{div} fs-lede
**Think Vercel for integrations and background jobs.** Write the integration, configure the environment, and deploy it — without designing the surrounding runtime from scratch. Webhooks, polling, queues, scheduled jobs, API clients, and observability are part of the execution model, not infrastructure you assemble yourself.
```

::::{div} fs-cta
:::{button-ref} getting-started/quickstart
:ref-type: doc
:color: primary
Get started →
:::
:::{button-link} https://github.com/flowstash/flowstash
:color: secondary
:outline:
GitHub
:::
::::

## Why should I care?

Every integration project starts the same way: a webhook receiver, a queue, a scheduler, retry logic, an HTTP client with auth, some logging you'll regret later. You end up designing a small distributed system before writing a single line of business logic — and then designing it again for the next project.

FlowStash collapses all of that into a standard runtime:

- **Integration code stays integration logic.** You write how data is fetched, transformed, and delivered. The framework handles execution, scheduling, queues, retries, context propagation, and telemetry.
- **Webhooks, polling, queues, and scheduled jobs are first-class concepts** — part of the execution model, declared with decorators, not separate infrastructure you assemble and connect yourself. [→ Scheduling & Ingress](concepts/scheduling-and-ingress.md)
- **Two simple services, always the same shape.** An **API** service receives webhooks and inbound requests; a **worker** executes queued tasks, scheduled jobs, retries, and polling. You never redesign this topology per project. [→ Architecture](concepts/architecture.md)
- **Third-party APIs are described and accessed consistently.** Authentication, base URLs, retries, timeouts, and observability use the same client infrastructure across every integration. [→ Clients](concepts/clients.md)
- **Local and production execution use the same code.** Run tasks directly while developing, submit them to a queue in production, or put them on a schedule — without rewriting the integration. [→ Tasks & Steps](concepts/tasks-and-steps.md)
- **Deployment is intentionally simple.** Every integration has a standard structure and runtime, so there's no new deployment model to invent — use the managed platform or host it yourself, with no application code changes. [→ Deployment](deployment/overview.md)
- **Observability is part of the execution model.** Runs, tasks, HTTP calls, payloads, errors, and retries are correlated automatically instead of being reconstructed from scattered logs. [→ Observability](concepts/observability.md)

The result: integrations stop being bespoke glue and become **portable, repeatable software units**.

## What it looks like

```python
from flowstash import ingress
from flowstash.pipelines import RecordsFeed, RecordData, feed_consumer
from flowstash.clients import get_client
from fastapi import Request

@ingress.webhook(integration="shop", pipeline="orders", path="/shop/orders")
async def order_webhook(request: Request):
    order = await request.json()
    await RecordsFeed.get("orders").publish(
        RecordData(record_id=order["id"], record_type="order", data=order)
    )
    return {"received": True}

@feed_consumer(feed_id="orders", subscription="erp-sync")
async def sync_to_erp(record: RecordData):
    erp = get_client("erp")
    await erp.request("POST", "/orders", json=record.data)
```

That's a complete pipeline: an authenticated inbound webhook, a deduplicated record feed, and a delivery step with retries, credential management, and full tracing — none of which you wrote.

## Where to start

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} 🚀 Quickstart
:link: getting-started/quickstart
:link-type: doc
A running integration in five minutes.
:::

:::{grid-item-card} 🧭 Architecture
:link: concepts/architecture
:link-type: doc
The packages, the topology, the backends.
:::

:::{grid-item-card} 🛠️ Building an Integration
:link: guides/building-an-integration
:link-type: doc
The end-to-end walkthrough.
:::

:::{grid-item-card} 🚢 Deployment
:link: deployment/overview
:link-type: doc
Local → self-hosted → managed.
:::

::::

## The packages

| Package | What it is |
|---|---|
| `flowstash` | meta package — installs everything below |
| `flowstash-lib` | the programming model: decorators, context, feeds, state, observability |
| `flowstash-clients` | typed API clients: HTTP, GraphQL, OData, auth |
| `flowstash-runtime` | the engine: FastAPI app builder, workers, backends |
| `flowstash-cli` | the `flowstash` command |

```{toctree}
:hidden:
:caption: Getting Started
getting-started/installation
getting-started/quickstart
getting-started/project-setup
```

```{toctree}
:hidden:
:caption: Concepts
concepts/architecture
concepts/tasks-and-steps
concepts/context
concepts/clients
concepts/feeds-and-pipelines
concepts/state
concepts/scheduling-and-ingress
concepts/observability
```

```{toctree}
:hidden:
:caption: Guides
guides/building-an-integration
guides/calling-external-apis
guides/working-with-feeds
guides/scheduling-tasks
guides/handling-webhooks
guides/managing-secrets-and-config
guides/testing-integrations
guides/monitoring-and-debugging
```

```{toctree}
:hidden:
:caption: Deployment
deployment/overview
deployment/local-development
deployment/dramatiq-backend
deployment/managed-gcp
deployment/ci-cd
```

```{toctree}
:hidden:
:caption: Reference
reference/cli
reference/configuration
reference/api/flowstash-lib
reference/api/flowstash-clients
reference/api/flowstash-runtime
```

```{toctree}
:hidden:
:caption: Examples & Community
examples/index
community/contributing
community/faq
```

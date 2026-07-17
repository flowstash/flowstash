# Examples

## The scaffold is the first example

Every `flowstash init` project ships with a small, working integration you can read and run immediately:

- a webhook publishing to a record feed (`src/api/routes/webhooks.py`)
- a scheduled task (`src/worker/tasks/tasks.py`)
- an ad-hoc task using a client (`src/shared/tasks/sharedTasks.py`)
- a typed client subclass over a public demo API (`src/shared/clients/client.py`)

## Worked examples in the docs

| Example | What it shows | Where |
|---|---|---|
| Order sync (shop → ERP) | webhook + feed + OAuth2 client + reconciliation poll | [Building an Integration](../guides/building-an-integration.md) |
| Webhook → feed → single & batch consumers | the full pipeline in ~30 lines | [Quickstart](../getting-started/quickstart.md) |
| Auth recipes (API key, OAuth2 variants, basic) | client YAML per scheme | [Calling External APIs](../guides/calling-external-apis.md) |
| Debounced ERP delivery | collapsing bursty updates | [Working with Feeds](../guides/working-with-feeds.md) |
| Cursor-based polling | durable state, at-least-once fetch | [Scheduling Tasks](../guides/scheduling-tasks.md) |
| End-to-end tests without infrastructure | respx, ASGI transport, inline task runs | [Testing Integrations](../guides/testing-integrations.md) |

## In the repository

The framework's own test suites double as executable examples — `packages/flowstash_lib/tests/test_integration_full.py` drives a complete webhook → feed → consumers flow through the real FastAPI app.

:::{admonition} Contributions welcome
:class: tip
Have a real-world integration pattern worth sharing? Open a PR adding it under `examples/` — see [Contributing](../community/contributing.md).
:::

# flowstash

**Think Vercel for integrations and background jobs.**

Write the integration, configure the environment, and deploy it — without designing the surrounding runtime from scratch. Webhooks, polling, queues, scheduled jobs, declarative API clients, and correlated observability are part of the framework's execution model, not infrastructure you assemble per project. The same code runs in-process on your laptop, on your own Redis + containers, or on managed cloud infrastructure.

This is the convenience meta package. Installing it gives you the full framework:

- `flowstash-lib` — the programming model: tasks, ingress, feeds, state, observability
- `flowstash-clients` — API clients with auth, retries, and masking
- `flowstash-runtime` — the FastAPI app builder and worker engine
- `flowstash-cli` — the `flowstash` command: scaffold, run, build, deploy

## Install

```bash
pip install flowstash
```

## Get started

```bash
flowstash init --name hello
python api_main.py
```

📚 **Documentation:** https://flowstash.github.io/flowstash/ — start with the [quickstart](https://flowstash.github.io/flowstash/getting-started/quickstart/).

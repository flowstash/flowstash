# flowstash-runtime

The runtime engine of the [FlowStash](https://flowstash.github.io/flowstash/) integration framework. It turns the declarations from `flowstash-lib` into running services:

- **`create_fastapi_app(config, auto_import=...)`** — the API service: webhook routes, health checks, your own FastAPI routers
- **`initialize_runtime(config, auto_import=...)` + `run_worker(config)`** — the worker service: tasks, schedules, feed consumers
- **Pluggable backends** — in-process `asyncio` (development), `dramatiq` over Redis (self-hosted), or `managed` (FlowStash platform on cloud infrastructure), selected by configuration

```python
from flowstash.config.env_loader import load_config_dir
from flowstash.runtime import create_fastapi_app

config = load_config_dir("config", environment="dev")
app = create_fastapi_app(config, auto_import=[Path("src/api/routes")])
```

## Install

```bash
pip install flowstash-runtime
```

Includes `flowstash-lib` and `flowstash-clients`. For the CLI as well, install the meta package: `pip install flowstash`.

📚 **Documentation:** [Architecture](https://flowstash.github.io/flowstash/concepts/architecture/) · [Deployment](https://flowstash.github.io/flowstash/deployment/overview/) · [API reference](https://flowstash.github.io/flowstash/reference/api/flowstash-runtime/)

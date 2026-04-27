# flowstash Agents Guide

This guide explains how to build AI Agents and robust integrations using flowstash. It covers the core concepts you need to know to bring data in, process it, and interact with external systems.

## Core Concepts

flowstash integrations revolve around the following core ideas:
- **Integration Context**: Automatically tracks the current integration, pipeline, run, and context across distributed operations.
- **Ingress**: How data enters your pipelines (via Webhooks or Polled schedules).
- **Clients**: How you make authenticated requests to external APIs.
- **Records Feed**: How you queue and deduplicate records for processing.
- **Project Structure**: How to organize code between API, Worker, and Shared modules.

## Bringing Data In: Ingress

You can trigger your pipelines through two main ingress decorators provided by `flowstash.ingress`:

### 1. Webhooks (`@ingress.webhook`)
Use webhooks when an external system can push events to your application. This decorator registers the handler as part of the integration but leaves the exact handling logic to you.

```python
from flowstash.ingress import ingress

@router.post("/webhook/slack")
@ingress.webhook(integration_pipeline="slack.messages", integration="slack")
async def handle_slack_webhook(request):
    data = await request.json()
    # Read the webhook and process the data!
```

### 2. Polling (`@ingress.poll`)
Use polling when you need to fetch data on a schedule. This decorator acts as a specialized scheduler entrypoint that injects a durable `state` dictionary into your function. This is perfect for remembering "watermarks" (like the last processed token or timestamp) to paginate stateful APIs.

The state is automatically saved for you as long as the function executes without exceptions.

```python
from flowstash.ingress import ingress
from datetime import datetime

@ingress.poll(integration_pipeline="slack.messages", integration="slack", schedule="*/5 * * * *")
async def poll_slack(state: dict):
    # Retrieve the watermark from the previous run
    since = state.get("since")
    
    # Fetch new data using the watermark...
    page = await fetch_messages_from_api(since=since)
    
    # Process or publish data
    
    # Update the watermark; it will be automatically saved!
    state["since"] = datetime.now()
```

## State Management: The State Machine Principle

Integrations often need to remember things between runs. flowstash treats every integration run as a transition in a **state machine**. You load the current state, perform your logic, and save the updated state.

### The `State` Facade

The `State` facade (accessible via `from flowstash.integration import State`) provides a centralized interface for persistent storage. It automatically handles namespacing, serialization, and TTLs, so you can focus on the data.

#### Scopes
State is automatically isolated into three logical scopes based on the active context:
- **`integration` (Default)**: Shared across all tasks and pipelines in a specific integration (e.g., `slack`). Use for global settings or cross-pipeline flags.
- **`pipeline`**: Shared across all tasks in a specific pipeline (e.g., `slack.sync_users`).
- **`ingress`**: Private state for a specific ingress entrypoint (e.g., a specific poll timer). This is the default scope for the `state` dict injected into `@ingress.poll`.

#### Basic Usage
```python
from flowstash.integration import State

@integration_task(integration="demo", integration_pipeline="example")
async def my_stateful_task():
    # 1. Get current state (scoped to integration:demo by default)
    # Returns None or the decoded value (JSON objects are automatically parsed)
    last_processed = State.get("last_id") or 0
    
    # ... perform logic ...
    
    # 2. Update state (persists to the configured backend: Redis, SQLite, Managed, etc.)
    State.set("last_id", 123)
```

### Auto-Saving in Ingress
In `@ingress.poll`, state management is handled for you. The `state` dictionary is automatically loaded before your function runs and persisted back to the `ingress` scope precisely when your function returns successfully.

```python
@ingress.poll(integration_pipeline="demo", integration="acme", schedule="* * * * *")
async def poll_handler(state: dict):
    # This 'state' is already loaded for you
    cursor = state.get("cursor", "init")
    
    # ... fetching data ...
    
    # Changes to this dict are automatically saved on a successful return
    state["cursor"] = "next_page_token"
```

### Context Bound
`State` is fully context-aware. You don't need to pass around tenant IDs or integration names. When you are inside an `@integration_task`, `@integration_step` or an `@ingress` handler, `State` already knows where to store the data.

If you need to use `State` in a CLI script or a test outside of a managed run, you can bind it manually:
```python
from flowstash.context import integration_context

with integration_context(integration="my-int", integration_pipeline="pipe"):
    # Inside this block, State is bound to "my-int" and "pipe"
    State.set("setup_complete", True)
```


## Making External Requests: Clients

flowstash makes it easy to interact with external APIs via Clients. **All client behavior—including authentication, base URLs, and retries—is driven by YAML configuration files.** This approach ensures that sensitive credentials and environment-specific settings are kept out of your code.

### Adding a Client

To use a client, you **must first** define its configuration in a YAML file (e.g., `config/shared/clients/slackClient.yaml`). flowstash uses these files to determine the base URL, authentication, and retry logic.

> [!IMPORTANT]
> **The Configuration File is Mandatory.**
> Every client you use via `get_client("myClient")` **must** have a corresponding configuration file. Without it, the client cannot be resolved. The name you pass to `get_client` must match either the filename (without extension) or the `client_id` specified inside the file.

#### The `clients.yaml` Pointer
The `clients.yaml` file in your shared configuration folder tells flowstash where to find your client definitions. Its schema is defined by `ClientsConfigPointer` in `flowstash.config.runtime_config`:

```yaml
path: clients       # Relative or absolute path to the clients directory
pattern: "*.yaml"   # Glob pattern to match client files (default: "*.yaml")
recursive: false    # If true, searches subdirectories (default: false)
```

#### Client Settings Schema
Each individual client file follows the `ClientSettings` schema defined in `flowstash.clients.config`. You can use either camelCase (default) or snake_case for field names.

Common fields:
- `baseUrl` (or `base_url`): The root URL for all requests.
- `timeout`: Request timeout in seconds (default: 10.0).
- `auth`: Authentication configuration.
- `retry`: Configuration for automatic retries.

#### Example: API Key Authentication
```yaml
client_id: myClient            # Optional: defaults to filename
baseUrl: https://api.example.com
auth:
  type: api_key
  key: "X-API-Key"             # Header or query parameter name
  value: "${MY_API_KEY}"       # Value (supports environment variables)
  in: header                   # Place in "header" (default) or "query"
```

#### Example: OAuth2 Authentication
```yaml
baseUrl: https://api.service.com
auth:
  type: oauth2
  client_id: "${CLIENT_ID}"
  client_secret: "${CLIENT_SECRET}"
  token_url: "https://auth.service.com/oauth/token"
  scopes: ["read", "write"]
  # clientAuthMethod controls how credentials are sent to the token endpoint:
  #   client_secret_basic  – Authorization: Basic header (default)
  #   client_secret_post   – credentials in the POST body
  clientAuthMethod: client_secret_basic
```

#### Example: TLS / Certificate Configuration
Use the `tls` block to configure mutual TLS (mTLS) client certificates or to customise server certificate verification.

```yaml
# Mutual TLS — present a client certificate to the server
baseUrl: https://internal.corp/api
tls:
  certFile: /certs/client.pem   # PEM-encoded client certificate
  keyFile: /certs/client.key    # Private key (omit if the key is bundled in certFile)

# Custom CA bundle — verify the server against a private / corporate CA
baseUrl: https://internal.corp/api
tls:
  caBundle: /certs/corp-ca.pem

# Disable server certificate verification (development / self-signed only)
baseUrl: https://localhost:8443
tls:
  verifySSL: false
```

All four fields are optional and can be combined:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `certFile` | string | — | Path to the PEM-encoded client certificate (enables mTLS) |
| `keyFile` | string | — | Path to the PEM-encoded private key (omit if bundled with `certFile`) |
| `caBundle` | string | — | Path to a custom CA certificate / bundle for server verification |
| `verifySSL` | bool | `true` | Set to `false` to skip server certificate verification (**dev/testing only**) |

You can simply define `clients/slackClient.yaml` and the system will expose it to your tasks.


### Using a Client
Once configured, you can retrieve the standard HTTP client (from the `flowstash.clients` package) anywhere.

```python
from flowstash.clients import get_client
from flowstash.decorators import integration_task

# Resolves the client named "slackClient" (expects slackClient.yaml configuration)
slack_client = get_client("slackClient")

@integration_task(integration="slack", integration_integration_pipeline="send_message")
async def send_slack_message_task(message: str):
    # Authorization and base URL logic are automatically handled here
    await slack_client.request("POST", "/chat.postMessage", json={"text": message})
```

### Custom Clients
If you have an API you interact with heavily, you can define a custom typed client. This encapsulates specific API routes for a better developer experience.

```python
from flowstash.clients import HttpClient, client
from typing import List

# Extending HttpClient and registering with the @client decorator
@client("demoClient") # Make sure the clientId ('demoClient') matches the clientId in yaml / config file name (if not specified in config yaml)
class DemoClient(HttpClient):
    """Custom client for the Demo API."""
    
    async def get_users(self) -> List[dict]:
        response = await self.request("GET", "/users")
        return response.json()
        
    async def get_user(self, user_id: int) -> dict:
        response = await self.request("GET", f"/users/{user_id}")
        return response.json()

    @classmethod
    def get(cls) -> DemoClient:
        return get_client("demoClient") 


# You can then resolve this custom client by name:
# demo = get_client("DemoClient")
```

## Logging

- **Don't use `print`**: Avoid `print()` for messages intended to be captured in production. Use the configured logger instead (for example, `from flowstash.logging import logger` or `logging.getLogger(__name__)`) so logs are collected and routed by the platform.
- **Don't log span start/end**: Tracing and instrumentation capture span lifecycle automatically — avoid logging explicit span start/end messages.
- **Use structured fields**: Include context fields like `integration`, `integration_pipeline`, and `run_id` to make logs easily filterable and correlated.
- **Choose levels appropriately**: Use `DEBUG` for verbose diagnostics, `INFO` for noteworthy events, and `ERROR`/`CRITICAL` for failures.

## Processing Data: Records Feed

When you receive payloads via Webhooks or Polling, you usually want to process them robustly and asynchronously on the backend. The `RecordsFeed` module provides an opinionated record queue with built-in deduplication that prevents overwhelming pipelines.

### Publishing Records
The framework resolves deduplication using a unique combination of `(integration, record_type, record_id)`. If multiple records arrive with the same identity, "latest-wins" semantics are applied (keeping the one with the latest timestamp).

It also seamlessly supports feeding massive data payloads -- large objects (> 5KB) are automatically offloaded to a BlobStore.

```python
from flowstash.pipelines.records_feed import RecordsFeed
from flowstash.pipelines.records_model import RecordData

# Retrieve your specific feed
feed = RecordsFeed.get(feed_id="slack.messages")

# A common pattern is inserting data retrieved via an `@ingress.poll` into a feed
await feed.publish(RecordData(
    record_id="msg_123",
    record_type="message",
    data={"text": "Hello World", "user": "U123"}
))
```

### Consuming Records
To process the data asynchronously, decorate a function with `@feed_consumer`. This supports robust configurations like automatic batched receiving, delays, rate-limiting, and concurrency control.

```python
from flowstash.pipelines import feed_consumer, RecordData

@feed_consumer(
    feed_id="slack.messages", 
    batch=True,
    max_batch_size=50,
    max_delay_ms=2000 # wait up to 2 seconds for the queue to fill
)
async def process_batch_messages(records: list[RecordData]):
    """
    Consumes a maximum of 50 records in a batch, waiting 
    up to 2 seconds for the queue to fill.
    """
    print(f"Began processing {len(records)} records.")
    for record in records:
        print(f"Record: {record.record_id} of type {record.record_type}")
        print(f"Data Payload: {record.data}")
```

```python
@feed_consumer(
    feed_id="slack.messages", 
    batch=False
)
async def process_batch_messages(record: RecordData):
    """
    Consumes a single record at a time.
    """
    print(f"Began processing {record.record_id} of type {record.record_type}")
    print(f"Data Payload: {record.data}")
```
## Project Structure

A typical flowstash project is organized into three main areas: Configuration, Deployment, and Source Code.

```text
.
├── config/                 # Application configuration
│   ├── shared/             # Base configuration for all environments
│   │   ├── backend.yaml    # Task backend settings (shared)
│   │   ├── clients.yaml    # Points to the clients folder
│   │   ├── clients/        # Shared client configurations
│   │   │   └── fooClient.yaml
│   │   └── .env            # Shared environment variables
│   └── local/              # Environment-specific overrides (e.g., local, dev, prod)
│       ├── backend.yaml    # Local-specific backend settings
│       └── .env            # Local-specific environment variables
├── deployment/             # deployment-related files
│   ├── shared/             # Base Dockerfiles
│   │   ├── api.Dockerfile
│   │   └── worker.Dockerfile
│   └── local/              # Local deployment configuration
│       └── docker-compose.yaml
├── src/                    # Source code for your integration
│   ├── api/                # API-specific code (Routes, Webhooks)
│   │   └── routes/
│   │       └── webhooks.py # typical place for @ingress.webhook ... we can also add the webhooks into purpose specific files ie slack_webhooks.py / stripe_webhooks.py etc
│   │       └── routes.py   # we can also add standard api routes ... like health check etc
│   ├── worker/             # Worker-specific code (Tasks, Consumers)
│   │   └── tasks/
│   │       └── xyzTask.py  # place for feed consumers, polling tasks, integration tasks etc
│   └── shared/             # Shared code (Models, Clients, Utils)
│       ├── clients/        # here should to the custom clients
│       └── models/         # here should to the custom models
├── api_main.py             # API Entry point
└── worker_main.py          # Worker Entry point
```

### Source Organization (`src/`)

flowstash separates code based on where it executes to ensure clean isolation and efficient scaling:

1.  **API (`src/api/`)**: Code that runs on the webserver. Its primary role is to handle incoming requests, typically defined using the `@ingress.webhook` decorator.
2.  **Worker (`src/worker/`)**: Code that runs on the background worker. This is where the "heavy lifting" happens, including `@integration_task`, `@ingress.poll`, and `@feed_consumer` handlers.
3.  **Shared (`src/shared/`)**: Logic used by both the API and Worker, such as custom typed Clients, Pydantic models, and utility functions.

### Configuration & Environments

flowstash uses a tiered configuration system that allows for seamless transitions between environments:

- **Inheritance**: Configuration is loaded from `config/shared/` first, and then overridden by environment-specific files in `config/{env}/`.
- **Backend Setup**: The `backend.yaml` file defines how tasks are executed (e.g., `asyncio` for local dev or `dramatiq` or `managed` for production).
- **Environment Variables**: `.env` files in both the shared and environment folders are automatically resolved and injected into the application.

### Execution Entry Points

- **`api_main.py`**: The entry point for the web server. It sets up the FastAPI application and automatically imports modules from `src/api` to register routes.
- **`worker_main.py`**: The entry point for the background process. It connects to the task backend (like Redis) and automatically imports modules from `src/worker` to register task signatures.


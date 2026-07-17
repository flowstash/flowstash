# flowstash-cli

The `flowstash` command — the developer workflow for the [FlowStash](https://flowstash.github.io/flowstash/) integration framework:

```bash
flowstash init            # scaffold a complete project
flowstash run dev         # bring it up locally (Docker Compose)
flowstash client curl crm /contacts     # call a configured API client, auth applied
flowstash webhook listen  # capture real webhook payloads as test fixtures
flowstash login           # authenticate to the FlowStash platform
flowstash deploy          # cloud build + deploy to managed infrastructure
```

Also included: environment management (`flowstash env`), project linking, API-key management, deployment profiles, and webhook fixture replay.

## Install

```bash
pip install flowstash-cli
```

or get the full framework with `pip install flowstash`.

📚 **Documentation:** [CLI reference](https://flowstash.github.io/flowstash/reference/cli/) · [Quickstart](https://flowstash.github.io/flowstash/getting-started/quickstart/)

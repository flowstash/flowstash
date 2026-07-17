# Installation

## Requirements

- Python **3.11+**
- Docker (optional — for `flowstash run` via Docker Compose and for the Dramatiq backend's Redis)

## Install

The `flowstash` meta package installs the full framework and the CLI:

::::{tab-set}
:::{tab-item} uv
```bash
uv add flowstash
# or, for a global CLI:
uv tool install flowstash
```
:::
:::{tab-item} pip
```bash
pip install flowstash
```
:::
::::

Verify:

```bash
flowstash version
```

## Installing à la carte

Only need part of the stack? The packages are published independently:

| Install | When |
|---|---|
| `flowstash` | building integration projects (recommended default) |
| `flowstash-clients` | you only want the HTTP/GraphQL/OData clients with auth in another codebase |
| `flowstash-lib` | library use of decorators, feeds, and state without the runtime |
| `flowstash-runtime` | includes `lib` + `clients`; the deployable runtime without the CLI |
| `flowstash-cli` | just the `flowstash` command |

## Next

Head to the [Quickstart](quickstart.md) to scaffold and run your first integration.

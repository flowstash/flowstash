# Contributing

Thanks for your interest in improving FlowStash! This page covers everything you need to get a development environment running and land a change.

## Repository layout

FlowStash is a monorepo managed as a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/). All publishable packages live under `packages/`:

| Package | Path | Depends on |
|---|---|---|
| `flowstash-clients` | `packages/flowstash_clients` | — |
| `flowstash-lib` | `packages/flowstash_lib` | `clients` |
| `flowstash-runtime` | `packages/flowstash_runtime` | `lib`, `clients` |
| `flowstash-cli` | `packages/flowstash-cli` | — |
| `flowstash` (meta) | `packages/flowstash` | `runtime`, `cli` |

Local path dependencies are pre-wired, so changes in one package are immediately visible to the others — no reinstalling between edits.

## Development setup

```bash
git clone https://github.com/flowstash/flowstash
cd flowstash
uv sync             # installs all workspace packages into .venv
```

Requires Python ≥ 3.11. `make install` (Poetry-based) also works if you prefer it.

## Everyday commands

```bash
make test           # run the test suite
make lint           # lint all packages
make format         # auto-format
```

Please run `make lint` and `make test` before opening a pull request.

## Documentation

Docs live in `docs/` and are built with [Sphinx](https://www.sphinx-doc.org/) + the [Furo](https://pradyunsg.me/furo/) theme, written in Markdown via [MyST](https://myst-parser.readthedocs.io/). Docs tooling is a [uv dependency group](https://docs.astral.sh/uv/concepts/projects/dependencies/#dependency-groups), so no separate virtualenv or requirements file is needed:

```bash
uv sync --group docs
uv run sphinx-autobuild docs docs/_build/html            # live-reload preview at http://127.0.0.1:8000
uv run sphinx-build -b html -W docs docs/_build/html     # what CI runs — warnings are errors
```

The API reference is generated from docstrings via Sphinx `autodoc` — improve the docstrings in the source, not the generated pages.

## Pull request guidelines

- Keep PRs focused: one logical change per PR.
- Add or update tests for any behavior change.
- Update relevant docs pages in the same PR.
- Describe *why* the change is needed, not just what it does.

## Release process

Releases publish all packages under a single shared version. The publish order is mandatory so PyPI dependencies resolve:

1. `flowstash-clients`
2. `flowstash-lib`
3. `flowstash-runtime`
4. `flowstash-cli`
5. `flowstash` (meta)

Before publishing:

```bash
make release-check   # verify tests and builds pass
make build           # produce sdist + wheel for each package
```

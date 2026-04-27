
# TODO write nicer
- install: poetry install / uv install

- form
  





# flowstash 

Welcome to the flowstash . This repository contains multiple publishable packages for the flowstash managed integration framework.

## Package Map & Dependency Graph

1. **flowstash-clients**: Lowest layer. Shared client types, API clients, transport, auth helpers, etc.
2. **flowstash-lib**: Depends on `flowstash-clients`. Shared domain logic/utilities reusable outside runtime.
3. **flowstash-runtime**: Depends on `flowstash-lib` + `flowstash-clients`. Actual runtime engine / bootstrap layer.
4. **flowstash-cli**: Independent CLI tools.
5. **flowstash** (meta): Convenience package that installs both runtime and CLI.

**Dependency Graph**:
* `clients` (independent)
* `lib` → `clients`
* `runtime` → `lib` + `clients`
* `cli`
* `flowstash` (meta) → `runtime` + `cli`

## Dev Workflow

The repository relies on a root `Makefile` for developer workflows. Note that local path dependencies are configured in each package for seamless local development.

* **Install all packages** (in dependency order):
  ```bash
  make install
  ```
* **Run tests**:
  ```bash
  make test
  ```
* **Linting / Formatting**:
  ```bash
  make lint
  make format
  ```

## Release Process

When publishing to PyPI, the release order is mandatory to ensure dependencies resolve correctly:

1. `flowstash-clients`
2. `flowstash-lib`
3. `flowstash-runtime`
4. `flowstash-cli`
5. `flowstash` (meta)

Before publishing:
1. Ensure all packages share the same version tag (e.g. `0.3.0`).
2. Run `make release-check` to verify tests and builds pass.
3. Run `make build` to generate `sdist` and `wheel` distribution files.

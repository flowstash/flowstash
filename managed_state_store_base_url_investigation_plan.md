# ManagedStateStore Base URL Investigation Plan

## Problem

`ManagedStateStore._base_url` is observed as `localhost:8080` even though Cloud Run is configured with:

```yaml
- name: FLOWSTASH_API_URL
  value: https://api.flowstash.dev
```

## What The Current Code Actually Does

### Resolution flow

1. `StateHandle._get_store()` in `packages/flowstash_lib/src/flowstash/integration/state.py` calls `registry._config.state_store.build_store()`.
2. `StateStoreConfig.build_store()` in `packages/flowstash_lib/src/flowstash/config/runtime_config.py` creates `ManagedStateStore(base_url=cfg.base_url, api_key=cfg.api_key)` when `state_store.type == managed`.
3. `ManagedStateStore.__init__()` in `packages/flowstash_lib/src/flowstash/state/stores/managed_store.py` sets:

   ```python
   self._base_url = (
       base_url or os.environ.get("FLOWSTASH_API_URL", "https://api.flowstash.dev")
   ).rstrip("/")
   ```

## Key Finding
 The startup sequence you captured changes this conclusion:

- You print `FLOWSTASH_API_URL` and `FLOWSTASH_API_KEY` **before** calling `load_config_dir()`.
- `load_config_dir()` then executes `load_dotenv(shared_env_file, override=True)` and `load_dotenv(env_env_file, override=True)`.
- Only **after that** do you call `initialize_runtime(config)` and later `registry._config.state_store.build_store()`.

 So the most likely explanation is now straightforward: the process environment is being overwritten inside `load_config_dir()` by a `.env` file, after your initial startup print and before `ManagedStateStore` is instantiated.
There is no code in this repository that hardcodes `localhost:8080` into `ManagedStateStore`.

Production tracing also established this:

- `os.getenv("FLOWSTASH_API_URL")` is `https://api.flowstash.dev` at request time.
- `ManagedStateStore._base_url` is already `http://localhost:8080` when `_request()` runs.
- `ManagedStateStore._api_key` is already empty when `_request()` runs.
- `base_url=None`
 The strongest candidate is now the `.env` loading behavior in `load_config_dir()`.
- `os.environ.get("FLOWSTASH_API_URL")`
- `os.environ.get("FLOWSTASH_API_KEY")`

Since the request-time environment is correct but the instance fields are wrong, the highest-probability explanation is now code/import skew in the deployed container.

That means `_base_url` can become `localhost:8080` only if one of these is true at runtime:

1. `ManagedStateStore` is called with a truthy `base_url="localhost:8080"`.
2. The Python process environment seen by this code has `FLOWSTASH_API_URL=localhost:8080` at the moment the store is constructed.

## Most Likely Source Of The Override

The strongest candidate is now **not** runtime config. It is a mismatch between the code you are reading locally and the code actually imported in production.


- `config.state_store.managed` is `None`, so `StateStoreConfig.build_store()` should call `ManagedStateStore(base_url=None, api_key=None)`.
- In the source file you shared, that should resolve `_base_url` from `FLOWSTASH_API_URL` and `_api_key` from `FLOWSTASH_API_KEY`.
- Your request-time monkey patch shows the process environment contains both of those values correctly.
- Yet the live instance fields are `http://localhost:8080` and empty string.

Those four facts are inconsistent with the local source code and consistent with one of these deployment-only conditions:

1. production imported a different `ManagedStateStore` implementation
2. production imported an older package version
3. production is running a different file than the one in this repo

The live process is initialized via:

environment = os.getenv("ENVIRONMENT", "dev")
config = load_config_dir("config", environment=environment)
runtime = initialize_runtime(config, ...)
```

With `ENVIRONMENT=prod`, the effective config source is the application working directory:

- `config/shared/backend.yaml`
- `config/prod/backend.yaml`
- `config/shared/.env`
- `config/prod/.env`
- any root `.env` discovered by the bare `load_dotenv()` call

This means the likely override is **not** in the explicit `state_store.managed` config block. The remaining likely sources are deployed code identity and import resolution inside the container.

## Falsifiable Hypotheses
### Hypothesis A: production is importing a different `ManagedStateStore` than expected

This now best matches the evidence.

Cheap check:

Add these prints in the live container before patching anything else:

```python
import inspect
import flowstash.state.stores.managed_store as managed_store_module

print("managed_store module file:", managed_store_module.__file__)
print("ManagedStateStore init file:", ManagedStateStore.__init__.__code__.co_filename)
print("ManagedStateStore request file:", ManagedStateStore._request.__code__.co_filename)
print(inspect.getsource(ManagedStateStore.__init__))
```

Expected discriminator:

- If the printed source or filename does not match `packages/flowstash_lib/src/flowstash/state/stores/managed_store.py`, the deployed container is running different code.
Cheap check:

Print these values:
print("init module:", ManagedStateStore.__init__.__module__)
print("class module:", ManagedStateStore.__module__)
```

Expected discriminator:

- If these point somewhere unexpected, another patch layer replaced the constructor.
This is plausible because `load_config_dir()` calls `load_dotenv()` and later calls `load_dotenv(shared_env_file, override=True)` and `load_dotenv(env_env_file, override=True)`.

Cheap check:

Log `os.environ.get("FLOWSTASH_API_URL")` at process startup and again immediately before constructing `ManagedStateStore`.

Expected discriminator:

### Hypothesis D: the process/revision is not actually running the source revision you expect

Cloud Run service configuration may be correct while the container image still contains an older installed package or stale source tree.

Cheap check:

print("flowstash package file:", flowstash.__file__)
```

Expected discriminator:

- If package/module file paths point into an unexpected site-packages location or stale artifact path, the deployment image contents are the problem.


`packages/flowstash_runtime/src/flowstash/runtime/wiring/runtime.py` resolves the managed backend API URL separately using:

```python
os.getenv("FLOWSTASH_API_URL") or os.getenv("MANAGED_API_URL") or "https://api.flowstash.dev"

But `ManagedStateStore` does **not** read `MANAGED_API_URL`; it only uses the explicit `base_url` argument or `FLOWSTASH_API_URL`.

This is not the cause of `localhost:8080`, but it is a config inconsistency worth fixing.


Pros:

- Fastest way to prove whether config or env is winning.
- Minimal code change.

Cons:

- Reactive only.
- Does not prevent future ambiguity.

### Option 2: Add explicit precedence logging and validation

Approach:

- Keep a small permanent debug/info log when managed state store config is resolved.
- Warn when both `cfg.base_url` and `FLOWSTASH_API_URL` are present and differ.

Pros:

- Makes this class of failure obvious in production.
- Low-risk and cheap to maintain.

Cons:

- Adds some startup noise unless logging is gated.

### Option 3: Standardize managed API URL resolution across runtime and state store

Approach:

- Introduce a helper such as `resolve_managed_api_url(explicit_base_url: Optional[str]) -> str`.
- Use it from `ManagedStateStore`, runtime wiring, worker entrypoints, and observability managed-store wiring.
- Define and document one precedence order.

Suggested helper contract:

```python
def resolve_managed_api_url(
    explicit_base_url: Optional[str],
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    ...
```

Recommended precedence:

1. explicit config value
2. `FLOWSTASH_API_URL`
3. `MANAGED_API_URL`
4. framework default

Pros:

- Removes inconsistent behavior across subsystems.
- Easier to test and reason about.

Cons:

- Slightly broader change surface.

## Recommendation

Use Option 1 immediately, but change the first check: prove code identity before changing runtime config resolution.

Reasoning:

- The immediate problem is diagnostic: we need to prove whether the production container is executing the same `ManagedStateStore` source code that exists in this repo.
- The broader issue is architectural inconsistency: different parts of the codebase resolve the managed API URL differently.

Based on the latest production evidence, the highest-probability direct cause is now:

1. `StateStoreConfig.build_store()` called `ManagedStateStore(base_url=None, api_key=None)` as expected.
2. The `ManagedStateStore.__init__` actually running in prod is not the same implementation as the file you are inspecting locally.
3. That deployed implementation set localhost/empty credentials by default or via an older code path.

## Recommended Implementation Plan

### Step 1

Add temporary logging in the live process to prove code identity before editing library logic.

Log these fields:

- `ManagedStateStore.__module__`
- `ManagedStateStore.__init__.__code__.co_filename`
- `ManagedStateStore._request.__code__.co_filename`
- `inspect.getsource(ManagedStateStore.__init__)`
- `flowstash.__file__`

Also add one temporary log immediately after `config = load_config_dir("config", environment=environment)` in the app startup module:

- `repr(config.state_store.type)`
- `repr(config.state_store.managed)`
- `Path("config").resolve()`

### Step 2

Add temporary logging in `packages/flowstash_lib/src/flowstash/state/stores/managed_store.py` inside `ManagedStateStore.__init__()`.

Log these fields:

- incoming `base_url`
- `os.environ.get("FLOWSTASH_API_URL")`
- final `self._base_url`

### Step 3

Deploy and inspect container logs for one failing request path.

Decision table:

- If `ManagedStateStore.__init__.__code__.co_filename` or printed source differs from the repo file, fix the deployment artifact or import path.
- If printed source matches exactly, then log inside `ManagedStateStore.__init__()` itself to capture raw inputs at construction time.
- If `config.state_store.managed is None` and `__init__` source matches exactly, but constructor inputs still produce localhost/empty auth, then the process environment was different at construction time and something in-process changed `os.environ` afterward.

### Step 4

Implement `resolve_managed_api_url(explicit_base_url, env=None)` in a shared module, likely under `packages/flowstash_lib/src/flowstash/config/`.

Update these call sites to use it:

- `packages/flowstash_lib/src/flowstash/state/stores/managed_store.py`
- `packages/flowstash_runtime/src/flowstash/runtime/wiring/runtime.py`
- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`
- `packages/flowstash_lib/src/flowstash/observability/registry.py`

### Step 5

Add tests.

New tests to add:

- `packages/flowstash_lib/tests/test_state_store_managed_config.py`
  - explicit `base_url` overrides env
  - `FLOWSTASH_API_URL` is used when explicit config is absent
  - `MANAGED_API_URL` fallback works if adopted
  - mismatch warning is emitted when both values are set and differ

- extend `packages/flowstash_lib/tests/test_config_loader.py`
  - verify `state_store.managed.base_url` from YAML wins over env
  - verify `.env` override behavior if that behavior remains intentional

## TODO

- [ ] Print the live module path for `flowstash.state.stores.managed_store`.
- [ ] Print the live source of `ManagedStateStore.__init__()`.
- [ ] Confirm whether the deployed container is running the expected code revision.
- [ ] If code matches, log constructor inputs inside `ManagedStateStore.__init__()`.
- [ ] Only if constructor inputs are correct, revisit environment mutation timing.
- [ ] Standardize managed API URL resolution in one helper.
- [ ] Update all managed API call sites to use the helper.
- [ ] Add regression tests for precedence behavior.
- [ ] Remove temporary diagnostic logging after confirmation.

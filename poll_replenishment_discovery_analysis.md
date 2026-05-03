# `poll_replenishment` Discovery Analysis — Managed Backend Fix Plan

## Summary

The hypothesis is **partially confirmed** but needs to be split by backend type. The root cause differs depending on which backend is active.

---

## How `@ingress.poll` registers a scheduled job

1. `@ingress.poll(...)` wraps the function in `state_wrapper`, then passes it to `TaskWrapper.__init__` ([ingress.py](packages/flowstash_lib/src/flowstash/ingress.py#L88))
2. `TaskWrapper.__init__` calls `register_task_wrapper(self)` and, because `default_schedule` is set, calls `register_task_schedule(self, schedule)` ([decorators.py](packages/flowstash_lib/src/flowstash/decorators.py#L115))
3. `register_task_schedule` checks `_backend`. If no backend yet → appended to `_pending_schedules`. If backend exists → calls `backend.register_schedule(...)` immediately ([backend.py](packages/flowstash_lib/src/flowstash/queue/backend.py#L89))

The decoration step is import-time, so everything above runs when the module is first imported.

---

## Startup order in `initialize_runtime`

([wiring/runtime.py](packages/flowstash_runtime/src/flowstash/runtime/wiring/runtime.py#L157))

```
Step 1: set_observability_config
Step 2: set_global_registry
Step 3: _auto_import_path  ← imports happen HERE, before backend is set
Step 4: set_backend(...)   ← drains _pending_schedules, then calls configure_task
```

When a poll task is imported at step 3, no backend exists yet, so its schedule lands in `_pending_schedules`.

When `set_backend` runs at step 4 ([backend.py line 62](packages/flowstash_lib/src/flowstash/queue/backend.py#L62)):

```python
while _pending_schedules:
    func, schedule, args, kwargs, tags = _pending_schedules.pop(0)
    backend.register_schedule(func, schedule, ...)   # ← _backend_handler is still None here

# THEN configure_task is called for each registered wrapper
if hasattr(backend, 'configure_task'):
    for wrapper in _registered_task_wrappers:
        backend.configure_task(wrapper)              # ← sets _backend_handler
```

**The ordering smell is real:** `_pending_schedules` are drained **before** `configure_task` is called.  
In `DramatiqBackend.register_schedule`, when `func._backend_handler is None`:

```python
actor_func = func   # falls back to the TaskWrapper, NOT the Dramatiq actor
scheduled_job_id = f"{actor_func.__module__}.{actor_func.__name__}"
```

`functools.update_wrapper` on `TaskWrapper` copies `__module__` and `__name__` from the underlying function, so `scheduled_job_id` is computed correctly. The `func` reference in `_scheduled_jobs` is the TaskWrapper object itself (not a copy), so by the time APScheduler fires, `func._backend_handler` is already set by the subsequent `configure_task` call. **The ordering concern does not cause a functional failure for schedules.**

---

## Confirmation/Refutation by backend

### ASYNC backend — hypothesis FULLY CONFIRMED

`initialize_runtime` for ASYNC **does not** start APScheduler.  
APScheduler is only started inside `_make_async_lifespan` ([ingress/app.py](packages/flowstash_runtime/src/flowstash/runtime/ingress/app.py#L45)):

```python
if scheduling_enabled and getattr(rt.backend, "_scheduled_jobs", None):
    scheduler = AsyncIOScheduler()
    ...
    scheduler.start()
```

This lifespan only runs when `create_fastapi_app` builds the FastAPI application. Starting a plain worker runtime (`initialize_runtime`) for an ASYNC backend **will not schedule any `@ingress.poll` tasks**. The hypothesis is correct.

Also note: for ASYNC backend, `initialize_runtime` imports `worker_main` **after** `set_backend` (the special ASYNC branch at line 248). But tasks imported via the `auto_import` argument are still imported at step 3 — before the backend. This inconsistency means the auto_import path and the ASYNC-specific worker_main import behave differently. This is the secondary startup-order smell.

### DRAMATIQ backend — hypothesis PARTIALLY REFUTED

`DramatiqConsumer.start()` **does** read `_backend._scheduled_jobs` and starts a `BackgroundScheduler` ([dramatiq_consumer.py](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_consumer.py#L33)):

```python
if hasattr(_backend, '_scheduled_jobs') and _backend._scheduled_jobs:
    self.scheduler = BackgroundScheduler()
    for job_info in _backend._scheduled_jobs:
        ...
    self.scheduler.start()
```

So for Dramatiq, the worker **can** discover and run poll tasks — but only if:
1. The module containing `poll_replenishment` is included in `auto_import` when the worker is started
2. `DramatiqConsumer.start()` is actually called (not just `initialize_runtime`)

The most likely root cause for Dramatiq: the worker entrypoint's `auto_import` list only includes "regular" task directories and **omits the ingress tasks module** where `poll_replenishment` lives.

### MANAGED backend — hypothesis FULLY CONFIRMED

`ManagedTasksBackend.on_worker_init()` is a no-op ([managed_tasks.py](packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py#L255)):

```python
def on_worker_init(self) -> None:
    """With the pull model, schedules are fetched via worker HTTP endpoint.
    We do nothing here."""
    pass
```

`get_scheduled_jobs()` always returns `[]`. There is no APScheduler in the managed backend's worker path. Schedules are expected to exist externally in Cloud Scheduler. No local discovery of `poll_replenishment` as a cron job will ever happen from a worker process using this backend.

---

## Root Cause Summary

| Backend | Poll discovered by worker? | Actual reason poll_replenishment is missing |
|---|---|---|
| **ASYNC** | ❌ Never | `_make_async_lifespan` (only in `create_fastapi_app`) is the sole APScheduler entrypoint |
| **DRAMATIQ** | ✅ Yes, IF imported | Module with `poll_replenishment` is likely absent from worker's `auto_import` list |
| **MANAGED** | ❌ Never | No local APScheduler at all; schedules live in Cloud Scheduler |

---

## Secondary Issue: `_pending_schedules` drained before `configure_task`

In `set_backend` ([backend.py](packages/flowstash_lib/src/flowstash/queue/backend.py#L62)), the schedule drain loop runs before `configure_task`:

```python
# 1. drain schedules ← _backend_handler is None on TaskWrappers here
while _pending_schedules:
    backend.register_schedule(func, schedule, ...)

# 2. configure tasks ← sets _backend_handler
for wrapper in _registered_task_wrappers:
    backend.configure_task(wrapper)
```

For Dramatiq, `register_schedule` with `_backend_handler=None` falls back to using the TaskWrapper itself as `actor_func`. Scheduled_job_id is computed from `wrapper.__module__`/`wrapper.__name__` (correct, via `functools.update_wrapper`). Since `job_info['func']` holds a reference to the same TaskWrapper object, `_backend_handler` is correctly resolved at APScheduler fire time. **No functional bug**, but the intent is fragile — it works only because Python object references are shared.

A safer implementation would be to call `configure_task` before draining `_pending_schedules`, or to defer schedule registration until after `configure_task` is done.

---

## Recommended Fix Paths

### If ASYNC backend
- Pass the ingress module(s) to `auto_import` in `create_fastapi_app`, not to `initialize_runtime` alone.
- Do not expect poll tasks to run from a standalone worker process; they must run inside the FastAPI lifespan.

### If DRAMATIQ backend
- Add the module/directory containing `poll_replenishment` to the `auto_import` list passed to the worker's `initialize_runtime` call.
- Verify `DramatiqConsumer.start()` is awaited in the worker entrypoint (not just `initialize_runtime`).

### Fix startup-order fragility (all backends)
- In `set_backend`, call `configure_task` before draining `_pending_schedules`:

```python
def set_backend(backend: TaskBackend):
    global _backend
    _backend = backend

    # Configure wrappers FIRST so _backend_handler is set before schedule registration
    if hasattr(backend, 'configure_task'):
        for wrapper in _registered_task_wrappers:
            backend.configure_task(wrapper)

    # Now drain pending schedules — _backend_handler is available
    while _pending_schedules:
        func, schedule, args, kwargs, tags = _pending_schedules.pop(0)
        backend.register_schedule(func, schedule, args, kwargs, tags)

    return backend
```

---

## Managed Backend: Root Cause and Fix Plan

The managed backend is designed so that `GET /schedules` returns all registered poll/task schedules, and an external scheduler (Cloud Scheduler) calls `POST /handle_task` on cron. Two bugs together prevent `poll_replenishment` from appearing in `/schedules`.

### Bug 1 — `create_app` never auto-imports task modules (primary)

**Location:** [packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/main.py](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/main.py)

`create_app(config)` does not accept an `auto_import` parameter. Its `startup_event` calls `initialize_runtime(config)` with no `auto_import` argument:

```python
# startup_event in main.py
initialize_runtime(config)   # no auto_import → task modules never imported
```

`@ingress.poll` only registers when the decorated module is imported. If the user's entrypoint does not explicitly import the task module, the `TaskWrapper` is never created, `register_task_schedule` is never called, and `_registered_tasks` stays empty.

`/schedules` reads `backend._registered_tasks` directly ([http_entrypoint.py line 43–46](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py#L43)) — so the result is always `{"tasks": []}`.

**Fix:** Add `auto_import` parameter to `create_app` and pass it through to `initialize_runtime`:

```python
# main.py
def create_app(config: RuntimeConfig, auto_import=None) -> FastAPI:
    ...
    @app.on_event("startup")
    async def startup_event():
        try:
            get_backend()
        except RuntimeError:
            initialize_runtime(config, auto_import=auto_import)  # ← pass through
```

The caller (user entrypoint) then passes:

```python
app = create_app(config, auto_import=[Path(__file__).parent / "tasks"])
```

---

### Bug 2 — `integration`/`pipeline` are always "unknown" in schedule dict

**Location:** [packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py](packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py#L218)

`ManagedTasksBackend.register_schedule` reads integration/pipeline from `tags`:

```python
task_dict = {
    ...
    "integration": (tags or {}).get("integration", "unknown"),  # ← always "unknown"
    "pipeline": (tags or {}).get("pipeline", "unknown"),        # ← always "unknown"
}
```

`tags` comes from `register_task_schedule` ← `TaskWrapper.__init__`:

```python
register_task_schedule(
    self,
    self.metadata["default_schedule"],
    tags=self.metadata.get("tags"),   # ← for @ingress.poll this key doesn't exist → None
)
```

The `@ingress.poll` `TaskWrapper` metadata is:
```python
{"integration": integration, "pipeline": pipeline, "name": ingress_name, "default_schedule": actual_schedule}
```
No `"tags"` key. The `integration` and `pipeline` values **are** on `func.metadata` but the backend reads the wrong field.

**Fix:** In `ManagedTasksBackend.register_schedule`, prefer `func.metadata` over `tags` for `integration`/`pipeline`:

```python
def register_schedule(self, func, schedule, args=None, kwargs=None, tags=None):
    # ... task_id computation unchanged ...

    # Read integration/pipeline from TaskWrapper metadata when available;
    # fall back to tags (used by integration_task with explicit tags).
    if hasattr(func, "metadata"):
        integration = func.metadata.get("integration") or (tags or {}).get("integration", "unknown")
        pipeline = func.metadata.get("pipeline") or (tags or {}).get("pipeline", "unknown")
    else:
        integration = (tags or {}).get("integration", "unknown")
        pipeline = (tags or {}).get("pipeline", "unknown")

    task_dict = {
        "task_id": task_id,
        "task_name": task_name,
        "target_url": f"{self.service_url}/handle_task",
        "integration": integration,
        "pipeline": pipeline,
        "default_schedule": schedule.cron,
    }
    self._registered_tasks.append(task_dict)
```

---

### End-to-end flow after fixes

1. User entrypoint: `app = create_app(config, auto_import=[tasks_dir])`
2. FastAPI startup: `initialize_runtime(config, auto_import=[tasks_dir])`
   - `_auto_import_path(tasks_dir)` imports the task module
   - `@ingress.poll` runs → `TaskWrapper.__init__` → `register_task_schedule(self, schedule, tags=None)` → `_pending_schedules.append(...)`
   - `set_backend(ManagedTasksBackend())` → drains `_pending_schedules` → `register_schedule(tw, schedule, tags=None)`
   - `register_schedule` reads `tw.metadata["integration"]`, `tw.metadata["pipeline"]` → correct values
   - `_registered_tasks` now contains the schedule dict with correct integration/pipeline
3. External system calls `GET /schedules` → returns `{"tasks": [{"task_id": "mymodule.poll_replenishment", "integration": "...", "pipeline": "...", "default_schedule": "* * * * *", ...}]}`
4. Cloud Scheduler calls `POST /handle_task` with `{"func_ref": "mymodule.poll_replenishment", ...}`
5. `_resolve_function("mymodule.poll_replenishment")` → dynamic import → gets the `TaskWrapper` → `await func.run()` → `state_wrapper` executes with state injection

---

### Todo

- [ ] **`main.py`**: Add `auto_import: Optional[List[Union[str, Path]]] = None` parameter to `create_app`; pass it to `initialize_runtime(config, auto_import=auto_import)` inside `startup_event`
- [ ] **`managed_tasks.py`**: Fix `register_schedule` to read `integration`/`pipeline` from `func.metadata` when available, falling back to `tags`
- [ ] **`backend.py`** (optional hardening): In `set_backend`, call `configure_task` before draining `_pending_schedules`

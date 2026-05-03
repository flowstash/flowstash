# Observability System — Enterprise-Grade Hardening Plan

## Overview

This document catalogs every structural weakness found in the observability pipeline, explains exactly why each is a problem, and describes what a correct, enterprise-grade solution looks like. Issues are grouped by severity.

---

## Severity Legend

| Severity | Meaning |
|---|---|
| 🔴 Critical | Silent data loss — events are dropped in normal operation |
| 🟠 High | Incorrect data — duplicate events, wrong semantics |
| 🟡 Medium | Reliability gap — events may be lost under load or on shutdown |
| 🔵 Low | Code quality / maintainability |

---

## Critical Issues — Silent Data Loss

### 🔴 ISSUE-1: `_fire_scheduled` Bypasses `AsyncManager` Entirely

**Location:** [`packages/flowstash_lib/src/flowstash/decorators.py`](packages/flowstash_lib/src/flowstash/decorators.py) — `TaskWrapper.submit()` (line ~163) and `TaskWrapper.schedule()` (line ~207)

**What it does today:**

Both methods define a local helper:

```python
def _fire_scheduled(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(coro)   # untracked fire-and-forget
        else:
            loop.run_until_complete(coro)
    except RuntimeError:
        asyncio.run(coro)
```

This is then called as: `_fire_scheduled(record__scheduled(correlation=ctx.corelation))`

**Why it is wrong:**

1. **Not tracked.** `AsyncManager._pending` only tracks futures submitted via `_submit()`. A task created with `asyncio.create_task()` inside a running loop is invisible to `AsyncManager.flush()`. The SCHEDULED event will be dropped on process exit or if the loop goes idle before it executes.
2. **Deprecated API.** `asyncio.get_event_loop()` raises a `DeprecationWarning` in Python 3.10+ when there is no running loop; it creates a new loop silently in 3.9 and below — behavior is version-dependent.
3. **Duplicate code.** The helper is copy-pasted identically in `submit()` and `schedule()`.
4. **Wrong fallback.** `asyncio.run(coro)` cannot be called from inside a running async context (it raises `RuntimeError: This event loop is already running`). The `RuntimeError` catch that wraps it only catches the `get_event_loop()` call, not the `asyncio.run` failure.
5. **SCHEDULED events are the only observability events that bypass the managed path.** Every other lifecycle event flows through `_enqueue_lifecycle()` → `AsyncManager._submit()` → tracked. SCHEDULED is the only one that does not.

**What the correct solution looks like:**

Replace both `_fire_scheduled` calls with `_enqueue_lifecycle`, exactly as `integration_context` does for STARTED/ENDED:

```python
from .observability.ingestion import _enqueue_lifecycle, record__scheduled

# in submit() and schedule():
_enqueue_lifecycle(record__scheduled, correlation=ctx.corelation, attrs={"delay": eta_or_delay})
```

This routes SCHEDULED events through the same tracked thread-pool path, makes them visible to `flush()`, and removes the duplicated local helper.

---

### 🔴 ISSUE-2: EVENTUAL Mode Silently Drops All Events in Dramatiq Workers

**Location:** [`packages/flowstash_lib/src/flowstash/observability/ingestion.py`](packages/flowstash_lib/src/flowstash/observability/ingestion.py) — `AsyncManager.execute()` (EVENTUAL branch) + [`packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py`](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py) — `AsyncRunner._build_strategy` `execute_fire` method

**The chain of failure:**

1. `FrameworkContextMiddleware.before_process_message` calls `_fire(record_run_started(...))`.
2. `_fire()` is `_async_runner.fire()` which calls `loop.run_until_complete(coro)`.
3. Inside `record_run_started`, `await AsyncManager.get_instance().execute(store.write_run_event, event)` is called.
4. `AsyncManager.execute()` in `EVENTUAL` mode does:
   ```python
   asyncio.create_task(_background())
   ```
5. `loop.run_until_complete(coro)` returns. The `_background` task is now in the per-thread loop's queue but **nobody ever runs it**.
6. `AsyncRunner` has no task-draining logic. The loop is never `.run_until_complete()` again until the next message. At that point the task may or may not execute depending on timing.
7. On worker shutdown the loop is never explicitly closed/drained — tasks are silently garbage-collected.

**Effect:** In `EVENTUAL` durability mode (the default), **every observability event emitted by a Dramatiq worker is silently dropped.**

The `_enqueue_lifecycle` path avoids this because it explicitly drains:
```python
pending = asyncio.all_tasks(loop)
if pending:
    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
```
But `_async_runner.fire()` does not.

**What the correct solution looks like:**

`AsyncRunner._build_strategy`'s `execute_fire` function must drain tasks after each `run_until_complete`:

```python
def execute_fire(coro):
    try:
        loop.run_until_complete(coro)
        # Drain any asyncio.create_task tasks scheduled by EVENTUAL mode
        pending = asyncio.all_tasks(loop)
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    except Exception:
        pass
```

This mirrors exactly what `_enqueue_lifecycle._run()` already does correctly.

---

### 🔴 ISSUE-3: `AsyncManager.flush()` Does Not Cover Actual Store Writes

**Location:** [`packages/flowstash_lib/src/flowstash/observability/ingestion.py`](packages/flowstash_lib/src/flowstash/observability/ingestion.py) — `AsyncManager.flush()`

**What `flush()` actually covers today:**

```
_enqueue_lifecycle()
  → AsyncManager._submit(_run)         ← _pending tracks this Future
      → _run() creates a new loop
      → loop.run_until_complete(record_run_started())
          → AsyncManager.execute(store.write_run_event, event)
              → loop.run_in_executor(_io_executor, _job)  ← UNTRACKED in _pending
                  → store.write_run_event(event)
                      → ApiEventsStore._AsyncWorker.submit(...)  ← UNTRACKED queue
                          → HTTP POST                             ← UNTRACKED delivery
```

`flush()` waits for `_pending` futures — the `_submit(_run)` level. By the time `_run` finishes, `_io_executor._job` has been *submitted* but may not be *complete*. And even when `_job` finishes, it has only placed the event in `ApiEventsStore._AsyncWorker`'s internal `queue.Queue`. The HTTP delivery happens asynchronously in the worker thread.

**Effect:** Calling `AsyncManager.flush()` does not guarantee that events were delivered to the store. It only guarantees that the lifecycle coroutines have been dispatched.

**What the correct solution looks like:**

A proper `flush()` must drain all three levels:

```python
def flush(self, timeout: float = 10.0) -> None:
    # 1. Wait for all _executor trampoline threads (existing)
    with self._lock:
        pending = list(self._pending)
    if pending:
        concurrent.futures.wait(pending, timeout=timeout)
    with self._lock:
        self._pending.clear()

    # 2. Wait for all _io_executor store-write threads
    self._io_executor.shutdown(wait=True, cancel_futures=False)
    # Re-create to allow continued use
    self._io_executor = concurrent.futures.ThreadPoolExecutor(max_workers=...)

    # 3. Flush the store's own internal queue
    try:
        store = get_events_store()
        if hasattr(store, "flush"):
            store.flush(timeout=timeout)
    except Exception:
        pass
```

Or simpler: expose a `flush_stores()` helper that calls `get_events_store().flush()` (already implemented as `ApiEventsStore.flush()`) and call it after the executor wait.

---

### 🔴 ISSUE-4: `execute_fire_and_forget` Is Invisible to `flush()`

**Location:** [`packages/flowstash_lib/src/flowstash/observability/ingestion.py`](packages/flowstash_lib/src/flowstash/observability/ingestion.py) — `AsyncManager.execute_fire_and_forget()` and its usage in `enqueue_log_event`

**What happens:**

`enqueue_log_event` (called for every captured log line) uses `execute_fire_and_forget`:
```python
AsyncManager.get_instance().execute_fire_and_forget(
    get_events_store().write_log, corr, level_name, message, log_attrs
)
```

`execute_fire_and_forget` submits to `_io_executor` with no tracking:
```python
self._io_executor.submit(_job)   # future is discarded
```

**Effect:** Log events are never tracked. `flush()` will not wait for them. On process shutdown, in-flight log events will be dropped. The more logs an integration emits, the more silently lost.

**What the correct solution looks like:**

Option A (preferred): Treat log events the same as run events — route through `_submit()` to track them:
```python
def execute_fire_and_forget(self, func, *args, **kwargs):
    def _job():
        try:
            func(*args, **kwargs)
        except Exception:
            pass
    self._submit(_job)  # tracked instead of self._io_executor.submit(_job)
```

Option B: Accept that logs are best-effort, but at minimum drain `_io_executor` in `flush()` so they are delivered if the process has time.

---

## High Severity — Incorrect / Duplicate Events

### 🟠 ISSUE-5: `FrameworkContextMiddleware` Emits Double STARTED and Double ENDED Events

**Location:** [`packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py`](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py) — `FrameworkContextMiddleware.before_process_message` and `after_process_message`

**What happens:**

`before_process_message` calls `ctx_mgr.__enter__()`. `integration_context.__enter__()` automatically calls `_enqueue_lifecycle(record_run_started, ...)` — that is by design. Then the middleware immediately fires *another* explicit `record_run_started`:

```python
ctx = ctx_mgr.__enter__()   # ← fires _enqueue_lifecycle(record_run_started)  [1]
# ...
_fire(record_run_started(correlation=..., scheduled_job_id=...))  # ← [2] DUPLICATE
```

Similarly in `after_process_message`:
```python
_fire(record_run_ended(...))           # ← fires [1]
# ...
self.local.ctx_mgr.__exit__(...)       # ← fires _enqueue_lifecycle(record_run_ended)  [2] DUPLICATE
```

**Effect:** Every Dramatiq task generates 2× STARTED and 2× ENDED events. Observability dashboards show inflated counts; timeline views break; status machines that transition on first-seen STARTED/ENDED get confused.

**What the correct solution looks like:**

`integration_context` should accept a `record_lifecycle: bool = True` parameter. When `False`, it manages context tokens (setting/resetting the context var) but skips auto-recording. Middleware sets `record_lifecycle=False` and owns all lifecycle recording itself:

```python
# integration_context signature change:
def __init__(self, ..., record_lifecycle: bool = True):
    self._record_lifecycle = record_lifecycle

# In __enter__:
if self._record_lifecycle:
    if self._is_root_run:
        _enqueue_lifecycle(record_run_started, ...)
    else:
        _enqueue_lifecycle(record_span_started, ...)

# In middleware:
ctx_mgr = integration_context(
    integration=integration,
    integration_pipeline=pipeline,
    run_id=run_id,
    tags=tags,
    record_lifecycle=False,   # middleware handles this explicitly
)
```

This cleanly separates concerns: the context manager handles state propagation, lifecycle recording is the caller's responsibility when `record_lifecycle=False`.

---

### 🟠 ISSUE-6: `http_entrypoint.py::handle_task` Emits Double STARTED and Double ENDED Events

**Location:** [`packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`](packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py) — `handle_task()`

**What happens:**

```python
with integration_context(...) as ctx:           # ← auto-records STARTED [1]
    await record_run_started(correlation=...)    # ← explicit STARTED [2] DUPLICATE
    try:
        ...
        await record_run_ended(status=..., ...)  # ← explicit ENDED [1]
    except Exception:
        await record_run_ended(status="FAILED") # ← explicit ENDED [1]
# __exit__ of integration_context fires record_run_ended via _enqueue_lifecycle [2] DUPLICATE
```

This is the same anti-pattern as ISSUE-5 but in the managed/HTTP backend.

**What the correct solution looks like:**

Same two options:
- (Preferred) Use `record_lifecycle=False` on `integration_context` and keep the explicit calls.
- Remove the explicit `record_run_started`/`record_run_ended` calls and rely on `integration_context` auto-recording (simpler, but loses `scheduled_job_id` injection on STARTED, which needs another mechanism).

---

### 🟠 ISSUE-7: `FrameworkContextMiddleware` Fires Double STARTED for Subtasks (Redundant `record_run_started` + `record_span_started`)

**Location:** `FrameworkContextMiddleware.before_process_message`, subtask branch

```python
else:
    _fire(record_run_started(correlation=..., status="RUNNING_SUBTASK"))  # ← run event
    _fire(record_span_started(name=..., correlation=...))                 # ← span event
```

A subtask fires both a `RUN_STARTED` (with status `RUNNING_SUBTASK`) and a `SPAN_STARTED`. The semantic intent is "subtask = span". Emitting both creates ambiguity: is a subtask a run or a span? Querying the store will surface it in both run and span tables.

**What the correct solution looks like:**

Decide on one semantic: subtasks are spans. Remove the `record_run_started(..., status="RUNNING_SUBTASK")` call for subtasks entirely. The `record_span_started` is sufficient and correct.

---

## Medium Severity — Reliability Gaps

### 🟡 ISSUE-8: No Flush at Task Completion in Worker Middleware

**Location:** `FrameworkContextMiddleware.after_process_message`

After `record_run_ended` is fired via `_async_runner.fire()`, the method returns immediately. At this point, with EVENTUAL mode and even with ISSUE-2 fixed (task draining added to `fire()`), there is no guarantee the `ApiEventsStore._AsyncWorker` has flushed its HTTP queue before Dramatiq pulls the next message (and potentially the process receives SIGTERM).

**What the correct solution looks like:**

After `after_process_message` lifecycle calls complete, call:
```python
from flowstash.observability.ingestion import AsyncManager
from flowstash.observability.registry import get_events_store

AsyncManager.get_instance().flush(timeout=5.0)
try:
    get_events_store().flush(timeout=5.0)
except Exception:
    pass
```

This should be gated behind a config flag `ObservabilityConfig.flush_on_task_exit: bool = True` so operators can opt out for high-throughput, low-criticality scenarios.

---

### 🟡 ISSUE-9: `_enqueue_lifecycle` Creates a New Event Loop Per Lifecycle Call

**Location:** `ingestion.py` — `_enqueue_lifecycle._run()`

Every lifecycle event (STARTED, ENDED, SPAN_STARTED, SPAN_ENDED) spawns a thread and creates a brand-new `asyncio.new_event_loop()` just to run a single coroutine. With many concurrent integrations or high-frequency steps, this can exhaust OS thread resources.

Additionally, each new loop cannot share async resources (connection pools, HTTP sessions) with any other loop — meaning `ApiEventsStore`'s synchronous approach (where the store uses a separate thread anyway) is fine, but if stores ever become async-native, this pattern would prevent connection reuse.

**What the correct solution looks like:**

Introduce a single persistent background event loop running in a dedicated daemon thread, and submit coroutines to it via `asyncio.run_coroutine_threadsafe()`. This is the standard pattern for "run async from sync":

```python
class _LifecycleEventLoop:
    """Single persistent loop for all lifecycle coroutine execution."""
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def submit(self, coro) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def flush(self, timeout: float = 10.0):
        """Wait for all submitted coroutines to complete."""
        # track futures and wait
```

`_enqueue_lifecycle` would then be:
```python
def _enqueue_lifecycle(coro_fn, *args, **kwargs):
    future = _lifecycle_loop.submit(coro_fn(*args, **kwargs))
    AsyncManager.get_instance()._track_future(future)
```

---

## Low Severity — Code Quality

### 🔵 ISSUE-10: Debug `print()` Statement in Production Store Code

**Location:** [`packages/flowstash_lib/src/flowstash/observability/stores/api_stores.py`](packages/flowstash_lib/src/flowstash/observability/stores/api_stores.py) — `_AsyncWorker.submit()`, line 75

```python
def submit(self, endpoint: str, payload: Any):
    print(f"Submitting event to {endpoint} with payload: {payload}")  # ← THIS
```

Every single observability event will `print()` its full JSON payload to stdout in production. This would generate gigabytes of stdout noise and expose potentially sensitive run data in process logs.

**Fix:** Delete the `print()` statement. If verbose logging is needed for debugging, replace it with `logger.debug(...)`.

---

### 🔵 ISSUE-11: `_fire_scheduled` Is Duplicated Identically in `submit()` and `schedule()`

**Location:** `decorators.py` — `TaskWrapper.submit()` and `TaskWrapper.schedule()`

The local function `_fire_scheduled` is defined identically in both methods. Beyond the code smell, it means any fix applied to one is easily missed in the other (as demonstrated by ISSUE-1).

**Fix:** Resolved entirely by fixing ISSUE-1 (replacing both with `_enqueue_lifecycle`). The local helper disappears.

---

### 🔵 ISSUE-12: `record__scheduled` Naming Is Inconsistent

**Location:** `ingestion.py`, `decorators.py`

All public lifecycle functions follow the pattern `record_<noun>_<verb>`:
- `record_run_started`
- `record_run_ended`
- `record_span_started`
- `record_span_ended`

The scheduled function uses a double underscore: `record__scheduled`. This was likely a placeholder that was never cleaned up. It should be `record_run_scheduled`.

**Fix:** Rename `record__scheduled` → `record_run_scheduled` throughout (3 files).

---

### 🔵 ISSUE-13: `AsyncManager` Has Two Thread Pools with Overlapping/Confusing Semantics

**Location:** `ingestion.py` — `AsyncManager.__init__`

```python
self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)    # for lifecycle trampolines
self._io_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)  # for store writes
```

- `_executor` is used only by `_submit()` (called from `_enqueue_lifecycle`)
- `_io_executor` is used by `execute()` and `execute_fire_and_forget()`
- `flush()` only waits on `_executor` futures (via `_pending`)
- `_io_executor` futures are untracked

The naming (`_executor` vs `_io_executor`) does not clearly communicate the ownership hierarchy. A developer reading the code must trace all call sites to understand what is and isn't flushed.

**Fix:** Consolidate into a single tracked executor. If IO writes must be isolated for performance, make that pool explicitly flushed in `AsyncManager.flush()` (see ISSUE-3). Rename `_executor` → `_lifecycle_executor` and `_io_executor` → `_store_executor` for clarity.

---

### 🔵 ISSUE-14: Module-Level `_config` Global Creates Test Interference Risk

**Location:** `ingestion.py` — top-level `_config = ObservabilityConfig()`

`_config` is a mutable module-level global changed by `set_observability_config()`. Tests that call `set_observability_config()` will affect all concurrent and subsequent tests unless they restore the original value. There is no context manager or reset mechanism.

**Fix:** Wrap in a `contextvars.ContextVar` or at minimum document that tests must save and restore. A `reset_observability_config()` helper or fixture would prevent test pollution.

---

## Summary Table

| # | Severity | Issue | File | Fix Complexity |
|---|---|---|---|---|
| 1 | 🔴 Critical | `_fire_scheduled` bypasses AsyncManager — SCHEDULED events dropped | `decorators.py` | Small — replace with `_enqueue_lifecycle` |
| 2 | 🔴 Critical | EVENTUAL mode drops all events in Dramatiq workers | `dramatiq_backend.py` | Small — add task drain to `execute_fire` |
| 3 | 🔴 Critical | `flush()` doesn't cover `_io_executor` or store queue | `ingestion.py` | Medium |
| 4 | 🔴 Critical | `execute_fire_and_forget` (log events) invisible to flush | `ingestion.py` | Small |
| 5 | 🟠 High | Double STARTED/ENDED in Dramatiq middleware | `dramatiq_backend.py` | Medium — add `record_lifecycle=False` param to `integration_context` |
| 6 | 🟠 High | Double STARTED/ENDED in `http_entrypoint.py` | `http_entrypoint.py` | Small |
| 7 | 🟠 High | Subtasks emit both RUN_STARTED and SPAN_STARTED | `dramatiq_backend.py` | Small — remove RUN_STARTED for subtasks |
| 8 | 🟡 Medium | No store flush on task completion | `dramatiq_backend.py` | Medium — gated config flag |
| 9 | 🟡 Medium | New event loop per lifecycle call — thread thrash | `ingestion.py` | Large — introduce persistent lifecycle loop |
| 10 | 🔵 Low | `print()` debug statement in production | `api_stores.py` | Trivial |
| 11 | 🔵 Low | `_fire_scheduled` duplicated | `decorators.py` | Resolved by #1 |
| 12 | 🔵 Low | `record__scheduled` naming inconsistency | `ingestion.py`, `decorators.py` | Trivial rename |
| 13 | 🔵 Low | Two thread pools with unclear flush semantics | `ingestion.py` | Medium |
| 14 | 🔵 Low | Module-level `_config` global — test interference | `ingestion.py` | Small |

---

## Recommended Implementation Order

### Sprint 1 — Stop the bleeding (critical, small effort)

1. **ISSUE-10**: Delete the `print()` in `api_stores.py`. (5 min)
2. **ISSUE-12**: Rename `record__scheduled` → `record_run_scheduled`. (15 min)
3. **ISSUE-1**: Replace `_fire_scheduled` in both `TaskWrapper` methods with `_enqueue_lifecycle`. (30 min)
4. **ISSUE-2**: Add asyncio task draining to `AsyncRunner.execute_fire` in `dramatiq_backend.py`. (30 min)
5. **ISSUE-6**: Remove explicit `record_run_started`/`record_run_ended` calls from `http_entrypoint.py::handle_task`. (20 min)
6. **ISSUE-7**: Remove the redundant `record_run_started(..., status="RUNNING_SUBTASK")` for subtasks in middleware. (15 min)

### Sprint 2 — Correctness (high, medium effort)

7. **ISSUE-5**: Add `record_lifecycle: bool = True` parameter to `integration_context`. Update middleware to pass `record_lifecycle=False`. Resolves double-recording for workers.
8. **ISSUE-3 + ISSUE-4**: Extend `AsyncManager.flush()` to drain `_io_executor` and call `get_events_store().flush()`.
9. **ISSUE-8**: Add `flush_on_task_exit` config flag; call `AsyncManager.flush()` + `store.flush()` in `after_process_message`.

### Sprint 3 — Robustness (medium, larger effort)

10. **ISSUE-9**: Replace per-call `asyncio.new_event_loop()` with a single persistent lifecycle loop thread. Simplifies `_enqueue_lifecycle` and removes thread-thrash.
11. **ISSUE-13**: Rename and consolidate the two thread pools; document flush semantics explicitly.
12. **ISSUE-14**: Protect `_config` against test interference with a context manager or reset helper.

---

## Key Design Principle for the Target State

The target architecture has one rule for how lifecycle events flow:

> **Every lifecycle event, regardless of where it is emitted (context manager, decorator, middleware, HTTP handler), must flow through a single tracked path that `flush()` can drain end-to-end.**

That path is:
```
record_* (async coroutine)
  → AsyncManager.execute (IMMEDIATE: awaited; EVENTUAL: tracked task)
    → _store_executor.submit(_job)    (tracked)
      → store.write_*(event)
        → ApiEventsStore._AsyncWorker.submit(...)
          → HTTP POST

flush():
  1. wait for all lifecycle trampolines (current _executor)
  2. wait for all _store_executor futures
  3. store.flush() — drain AsyncWorker queue + await HTTP delivery
```

No event should be able to escape this chain via `asyncio.create_task()`, a local `_fire_scheduled` helper, or an untracked `_io_executor.submit()`.

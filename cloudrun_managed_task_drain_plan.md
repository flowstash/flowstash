# Cloud Run Managed Task Drain Plan

## Goal

Make the managed worker safe during Cloud Run revision rollouts and scale-down so that a task already executing through `POST /handle_task` is allowed to finish whenever the platform still permits it, and never gets detached into background work that can be killed after the HTTP response is sent.

## Current Behavior

### What the code does today

1. User code delegates work through `TaskWrapper.submit()` in `packages/flowstash_lib/src/flowstash/decorators.py`.
2. `ManagedTasksBackend.submit()` in `packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py` sends the task definition to the managed API and returns a `ManagedJobHandle` immediately.
3. The managed API later dispatches the task to the worker service URL `.../handle_task`.
4. `handle_task()` in `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py` resolves the function and awaits task execution inline before returning the HTTP response.

### Important conclusion

The original caller is already fire-and-forget, but the Cloud Tasks callback is not. That is the correct durability boundary.

`handle_task()` currently keeps the Cloud Tasks HTTP request open until task execution finishes. That is desirable on Cloud Run. Returning early from `handle_task()` and continuing in background would weaken reliability, because Cloud Run explicitly does not guarantee background work outside the request lifecycle under request-based billing.

## External Constraints

### Cloud Run

From the Cloud Run container runtime contract and general development guidance:

- In-flight requests are normally given time to complete while traffic is drained from an instance.
- Cloud Run can send `SIGTERM` before shutdown and then `SIGKILL` roughly 10 seconds later.
- CPU is guaranteed while a request is being processed.
- Background work after the HTTP response is sent is not safe under request-based billing and should be avoided.

### Cloud Tasks

From the Cloud Tasks HTTP target overview:

- The worker must return `2xx` only after the task is actually complete.
- Non-`2xx`, timeout, or no response causes retry.
- Default HTTP deadline is 10 minutes, maximum 30 minutes.

## Problem Statement

The current implementation is directionally correct because it executes inline inside the request, but it is not explicit enough about shutdown behavior.

Current gaps:

1. There is no managed-worker drain controller that tracks active task requests.
2. There is no explicit "draining" state that prevents a new task from starting once shutdown begins.
3. `ManagedConsumer` does not document or explicitly tune graceful shutdown settings relative to Cloud Run's shutdown window.
4. There are no focused tests proving that `handle_task()` remains request-bound and that shutdown waits for active work.
5. There is no code-level or documented alignment between Cloud Run request timeout and Cloud Tasks dispatch deadline.

## Options

### Option 1: Keep request-bound execution and harden drain behavior

Approach:

- Keep `handle_task()` synchronous from the worker's point of view: do not return until the task is complete.
- Add active-request tracking and a draining flag.
- On shutdown, stop accepting new work and wait for active `handle_task()` executions to finish up to a bounded grace window.
- Keep observability flushes inside the request and during shutdown.

Pros:

- Smallest architectural change.
- Matches Cloud Tasks' delivery contract.
- Matches Cloud Run guidance to finish work before returning the response.
- Preserves existing fire-and-forget semantics for the original submitter.

Cons:

- Still bounded by Cloud Run request timeout, Cloud Tasks deadline, memory limits, and exceptional forced termination.
- Cannot guarantee arbitrarily long tasks.

### Option 2: Return early from `handle_task()` and continue in background

Approach:

- Accept the request, spawn background execution, return `202` or `200`, and finish later.

Pros:

- Short HTTP latency for the Cloud Tasks callback.

Cons:

- Not safe on Cloud Run services with request-based billing.
- Violates Cloud Tasks' intended success model.
- Risks task loss during rollout, scale-down, idle suspension, or process shutdown.

Recommendation:

- Do not implement this option.

### Option 3: Move execution to a dedicated worker runtime not tied to HTTP request lifetime

Approach:

- Keep Cloud Tasks only as ingress or move to another queue/broker, then execute inside a runtime designed for long-running work such as Dramatiq workers, Cloud Run worker pools, or Cloud Run jobs depending on workload shape.

Pros:

- Better fit if tasks can run longer than Cloud Tasks or Cloud Run HTTP timeouts.
- Clearer execution lifecycle for long-running workloads.

Cons:

- Much larger change.
- Requires deployment and operational redesign.
- May change observability, retry, and idempotency assumptions.

### Recommended approach

Implement Option 1 now, and explicitly document the boundary:

- For managed Cloud Run services, task execution must remain tied to the `/handle_task` request.
- If task runtime can exceed the configured request/deadline budget, move those workloads to Option 3 instead of trying to hide them behind background execution.

## Proposed Implementation

### 1. Introduce a managed drain controller

Create a new file:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/drain.py`

Add a new class:

- `ManagedTaskDrainController`

Suggested fields:

- `active_requests: int`
- `_draining: bool`
- `_lock: asyncio.Lock`
- `_idle_event: asyncio.Event`
- `_shutdown_started_at: Optional[datetime]`

Suggested methods:

- `async begin_request(kind: str, task_ref: str) -> bool`
  - Returns `False` if draining has already started.
- `async finish_request(kind: str, task_ref: str) -> None`
- `async start_draining(reason: str) -> None`
- `async wait_for_idle(timeout_s: float) -> bool`
- `def is_draining(self) -> bool`

Why this is needed:

- `handle_task()` needs a single authoritative place to decide whether the instance may start another task.
- Shutdown logic needs a single authoritative place to wait for active work to finish.

### 2. Wire the controller into the FastAPI app lifecycle

Update:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/main.py`

Changes:

1. Create one `ManagedTaskDrainController` during app construction and store it on `app.state.managed_task_drain_controller`.
2. Replace the current shutdown-only cleanup with a lifespan or shutdown sequence that:
   - marks the instance as draining,
   - waits for active task requests to complete,
   - flushes observability,
   - closes any managed feed backend clients.
3. Keep startup registration behavior intact.

Suggested new helper functions in `main.py`:

- `_build_managed_lifespan(config: RuntimeConfig) -> AsyncContextManager`
- `_shutdown_managed_runtime(app: FastAPI, timeout_s: float) -> None`

Why this file owns the controller:

- `create_app()` is the composition root for the managed worker.
- Shutdown coordination belongs at the app/runtime boundary, not inside individual handlers.

### 3. Make `handle_task()` explicitly request-bound and drain-aware

Update:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`

Refactor `handle_task()` into smaller helpers.

Suggested new helpers:

- `_get_drain_controller() -> ManagedTaskDrainController`
- `async _invoke_task_callable(func: Any, args: list, kwargs: dict) -> Any`
- `async _execute_managed_task(payload: TaskPayload, func_ref: str, func: Any) -> dict`

Behavior changes:

1. Before executing a task, ask the drain controller for admission.
2. If the instance is draining, return a retryable response immediately:
   - suggested status: `503 Service Unavailable`
   - suggested body: `{ "status": "DRAINING", "task": func_ref }`
3. If admitted, execute the task inline exactly as today.
4. Always release the controller slot in `finally`.
5. Keep the response open until:
   - the task finishes, and
   - observability flush completes.

Important design rule:

- Do not use `asyncio.create_task`, `BackgroundTasks`, daemon threads, or any other detached execution mechanism for the managed path.

### 4. Reuse the same controller for feed HTTP handlers

Update the same file:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`

Apply the same admission/finish pattern to:

- `kick_batched()`
- `deliver_classic()`

Reason:

- These endpoints are also Cloud Run HTTP-delivered workloads.
- If the requirement is "any currently running task will finish," the worker should not start new feed work during drain either.

### 5. Make graceful shutdown settings explicit in the managed consumer

Update:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/managed_consumer.py`

Recommended changes:

1. Configure `uvicorn.Config` with an explicit graceful shutdown timeout aligned to Cloud Run's signal window.
2. Add a short comment explaining that managed task execution must remain attached to the HTTP request and that the server is expected to drain in-flight requests.

Suggested constructor/config additions:

- `graceful_shutdown_timeout_s: int = 9`

Suggested config use:

- `uvicorn.Config(..., timeout_graceful_shutdown=self.graceful_shutdown_timeout_s, ...)`

The exact value should be chosen conservatively so shutdown bookkeeping finishes before Cloud Run's final kill window.

### 6. Align infrastructure time budgets

This repository does not appear to contain the actual Cloud Run service definition, so this work is partly outside the repo.

Required deployment checks:

1. Cloud Run request timeout must be at least the longest allowed task runtime.
2. Cloud Tasks dispatch deadline must be less than or equal to the Cloud Run request timeout and high enough for the expected task runtime.
3. Cloud Run concurrency should be intentionally chosen.
   - Use `1` for CPU-heavy or non-thread-safe task code.
   - Use higher values only if task handlers and shared state are concurrency-safe.
4. If cold start during rollout matters, consider `min-instances > 0`, but that does not replace drain handling.

### 7. Add focused tests

Create a new test file:

- `packages/flowstash_runtime/tests/test_managed_http_entrypoint.py`

Add tests with behavior-level names such as:

- `test_handle_task_waits_for_task_completion_before_returning`
- `test_handle_task_returns_503_when_instance_is_draining`
- `test_handle_task_releases_active_slot_on_failure`
- `test_kick_batched_respects_drain_controller`
- `test_deliver_classic_respects_drain_controller`

Add or extend runtime wiring tests in:

- `packages/flowstash_runtime/tests/test_runtime_wiring.py`

Suggested additional coverage:

- `test_managed_consumer_configures_graceful_shutdown_timeout`
- `test_create_app_initializes_managed_drain_controller`

Testing strategy:

- Use a controllable async task body with `asyncio.Event` to prove that the HTTP handler does not return before task completion.
- Simulate drain mode and assert a retryable `503` response for new work.
- Assert that active request counters return to zero on both success and failure.

## Flow After The Change

### Successful execution path

1. `ManagedTasksBackend.submit()` persists task creation to the managed API and returns immediately.
2. Cloud Tasks dispatches `POST /handle_task` to the worker.
3. `handle_task()` asks `ManagedTaskDrainController.begin_request("task", func_ref)` for admission.
4. The task executes inline within `integration_context(...)`.
5. Observability is flushed before the HTTP response is sent.
6. The handler returns `200` only after the work is actually complete.

### Shutdown path

1. Cloud Run starts draining the old revision.
2. The managed app marks itself draining.
3. New task requests receive `503`, causing Cloud Tasks to retry elsewhere later.
4. Existing admitted task requests are allowed to continue.
5. Shutdown waits up to the configured grace budget for active requests to finish.
6. Observability is flushed before process exit.

## Non-Goals

These changes do not guarantee completion for:

- tasks that exceed Cloud Run request timeout,
- tasks that exceed Cloud Tasks dispatch deadline,
- tasks killed by forced termination such as OOM,
- exceptional platform shutdown beyond the documented grace behavior.

If those cases matter, the workload should move to a non-HTTP-bound worker model.

## TODO

- Add `ManagedTaskDrainController` in `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/drain.py`.
- Store one controller instance on the managed FastAPI app in `create_app()`.
- Refactor `handle_task()` to use explicit admission, execution, and release helpers.
- Return `503` for new managed work once draining starts.
- Apply the same drain gate to `kick_batched()` and `deliver_classic()`.
- Add shutdown logic that marks drain, waits for idle, flushes observability, and closes clients.
- Make Uvicorn graceful shutdown timeout explicit in `ManagedConsumer`.
- Add focused runtime tests for request-bound execution and drain behavior.
- Document external deployment requirements for Cloud Run timeout, Cloud Tasks deadline, and service concurrency.

## Final Recommendation

Do not try to make `/handle_task` fire-and-forget on the worker side. The fire-and-forget boundary should remain at task submission to the managed API, not at Cloud Tasks delivery to the worker.

Implement drain-aware request-bound execution now. If any managed task is expected to outlive the Cloud Run or Cloud Tasks request budget, treat that as an architectural mismatch and move that workload to a true worker runtime instead of backgrounding it inside the Cloud Run service.
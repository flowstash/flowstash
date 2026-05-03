# Run Delegation And Observability Plan

## Goal

Make run lifecycle semantics match the execution model:

- A run ends when the current execution attempt finishes.
- Delegating future work does not keep the current run open.
- Every delegated unit is durably linked back to the run that created it before the current run ends.
- When delegated work actually executes, it creates a new `run_id` but preserves enough causal metadata to reconstruct the chain.

## Current Behavior Summary

### Where runs currently start and end

- `packages/flowstash_lib/src/flowstash/context.py`
  - `integration_context.__enter__()` auto-enqueues `record_run_started()` for root contexts.
  - `integration_context.__exit__()` auto-enqueues `record_run_ended()` for root contexts.
- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`
  - Managed task execution disables auto lifecycle and records `record_run_started()` / `record_run_ended()` manually.
- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py`
  - Dramatiq middleware also records lifecycle manually.

### Where delegation currently happens

- `packages/flowstash_lib/src/flowstash/decorators.py`
  - `TaskWrapper.submit()` and `TaskWrapper.schedule()` call `_enqueue_lifecycle(record_run_scheduled, ...)` before backend acceptance.
  - If there is no active context, they create a synthetic `IntegrationContext` but do not open a real run around the delegation itself.
- `packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py`
  - The managed backend serializes `context.run_id` into the future task payload as `run_id`.
- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`
  - `handle_task()` reuses `payload.run_id` as the execution run id instead of creating a new run id.
- `packages/flowstash_lib/src/flowstash/pipelines/records_feed.py`
  - Feed publish stores `source_run_id` and emits a `RecordLinkKind.PUBLISHED` link, but no first-class delegation edge.
- `packages/flowstash_lib/src/flowstash/pipelines/backends/managed_feed.py`
  - Managed feed publish behaves the same way over HTTP.

### Concrete gaps

1. `RUN_SCHEDULED` is being used as a loose proxy for delegation, but it does not encode a durable edge with target metadata, operation identity, or acceptance semantics.
2. Managed task execution currently reuses the trigger run's `run_id`, which collapses delegation and execution into one run.
3. Root `.submit()` / `.schedule()` calls outside an active run create no short-lived submission run at all.
4. Feed publication captures source metadata but not a reusable causal envelope for downstream execution.
5. `kick_batched()` ends its run before the `/ack` call succeeds, so the current barrier for a consumer attempt is too early.

## Invariants To Implement

1. A run represents one concrete execution attempt.
2. Delegated work always executes under a fresh `run_id`.
3. The direct parent execution is recorded as `parent_run_id`.
4. The original root trigger is preserved as `trigger_run_id`.
5. Every delegated unit gets a stable `operation_id`.
6. If a durable delegation edge is persisted, its identifier is stored as `causation_event_id` on the delegated unit.
7. A run may end only after every delegation created inside it has been durably accepted.

## Barrier Definition

These are the barriers that should control when the current run may end:

- Immediate step / `TaskWrapper.run()`: the wrapped function returns or raises.
- `TaskWrapper.submit()`: the backend returns an accepted handle or message id.
- `TaskWrapper.schedule()`: the backend returns an accepted handle and schedule metadata.
- Redis feed publish: the Lua publish script returns success.
- Managed feed publish: `/v1/feed/{feed_id}/publish` returns `2xx`.
- Managed batched consumer execution: the handler finishes and `/v1/feed/{feed_id}/ack` returns `2xx`.
- Managed classic consumer execution: the handler finishes and the HTTP request returns `200`.

## Implementation Options


A. Reuse existing run/span events and stuff everything into `attrs` - Dismissed
B. Add first-class delegation events for both tasks and feeds - Dismissed
Winner:
C. Hybrid: task handoff as spans, feed publish as lineage: Direct task submit/schedule stays a span with `DELEGATED` outcome; feed publish remains `RecordLink`-driven provenance.  Matches runtime semantics, gives UI the right shapes, avoids a new top-level store in phase.

### Recommendation

Option C.

The distinction should be explicit in the model:

- direct task submit/schedule is a real user-visible action inside the current run, so it should remain a span in that run with a clear terminal outcome of `DELEGATED`
- feed publish is not a true delegated execution request, so it should stay in the lineage graph rather than pretending to be the same thing as queue submission

This gives the UI two clear graph primitives:

- timeline spans for "what this run did"
- lineage edges for "what data later caused other runs"

## Recommended Design

### 1. Split true delegation from indirect lineage

Treat these as different concepts:

- True delegation:
  - `TaskWrapper.submit()`
  - `TaskWrapper.schedule()`
  - the current run asks another execution unit to run later or elsewhere
- Indirect lineage:
  - `RecordsFeed.publish()`
  - the current run publishes data that may later trigger some consumer run, but the relationship is mediated by record flow rather than a direct execution request

That semantic split should drive the storage model and the UI.

### 2. Extend causal context explicitly

Add the following fields to `IntegrationContext` in `packages/flowstash_lib/src/flowstash/context.py` and to `Correlation` in `packages/flowstash_lib/src/flowstash/observability/model.py`:

- `parent_run_id: Optional[str]` — direct parent run; sufficient for multi-hop chains since they are walked one edge at a time
- `operation_id: Optional[str]` — stable delegation identity generated before the backend is called; the primary join key between the delegation span and the child execution run

Propagation rule:

- Root in-process execution: both fields are `None`.
- When a run delegates: set `operation_id = uuid4()` at delegation time, `parent_run_id = current.run_id`; these are embedded in the backend payload and preserved on the child execution context.
- When delegated work executes: allocate a fresh `run_id`, copy `parent_run_id` and `operation_id` from the incoming payload.

`causation_event_id` and `job_id` were removed from `IntegrationContext` and `Correlation`; they are only needed on the delegation span attrs and in the transport payload, not on every lifecycle event.

### 3. Add lightweight transport metadata for true task delegation

Add a transport helper dataclass to `packages/flowstash_lib/src/flowstash/observability/model.py` or a nearby queue-specific module:

```python
@dataclass(frozen=True)
class TaskDelegationMetadata:
    parent_run_id: str
    operation_id: str
    target_task: str
    accepted_id: Optional[str] = None
    schedule_time: Optional[datetime] = None
    attrs: dict = field(default_factory=dict)
```

Field justification:

- `parent_run_id` — the run that performed the delegation; the child execution preserves this to enable the parent→child graph edge
- `operation_id` — a stable `uuid4()` generated at delegation time; survives queue transit and lets the UI link a delegation span to the execution run that later claims it, before any execution run id exists
- `target_task` — the function identity of the delegated task; auto-derived from the decorated function's module path, e.g. `myapp.tasks.sync_orders`; needed on the span so the UI can display what was handed off without waiting for the child run to appear
- `accepted_id` — the opaque id returned by the backend after acceptance, e.g. a Dramatiq message id, a Cloud Tasks task id, or a managed API task id; set after the backend call succeeds and used as a secondary join key if `operation_id` alone is not enough
- `schedule_time` — only present for `.schedule()` calls; the absolute or relative time the backend was asked to execute the task; displayed on the delegation span so the UI shows when the work is expected
- `attrs` — free-form pass-through for backend-specific metadata the caller wants to attach to the span without adding new typed fields

Fields deliberately excluded:

- `trigger_run_id` — removed; `parent_run_id` is enough because multi-hop chains can be walked one edge at a time
- `target_backend` — internal plumbing, not user-meaningful; the backend type can be inferred from the deployment context
- `queue_name` — backend-specific routing detail; store it in `attrs` if needed for a particular backend, not as a mandatory field on every delegation
- `scheduled_job_id` — this was a second id field that duplicated `accepted_id`; a cron job's stable identity belongs in `attrs["scheduled_job_id"]` when relevant, not as a promoted field

This is not a new top-level observability event. It is the shared causal envelope that lets:

- the parent span describe what it delegated
- the child execution run be linked back later when it starts

### 4. Model direct task handoff as a span with `DELEGATED` outcome

In `packages/flowstash_lib/src/flowstash/observability/ingestion.py`, add:

- `def build_task_delegation_metadata(*, ctx: IntegrationContext, target_task: str, schedule_time: Optional[datetime] = None, attrs: Optional[Dict[str, Any]] = None) -> TaskDelegationMetadata`
- `async def record_task_delegation_started(*, name: str, metadata: TaskDelegationMetadata, attrs: Optional[Dict[str, Any]] = None) -> None`
- `async def record_task_delegation_ended(*, name: str, metadata: TaskDelegationMetadata, status: str = "DELEGATED", error_summary: Optional[str] = None, attrs: Optional[Dict[str, Any]] = None) -> None`

Recommendation:

- Keep the actual execution lifecycle in `RunEvent` / `SpanEvent`.
- Record task submit/schedule as a short-lived span with `attrs` like:
  - `fw.span_kind = "delegation"`
  - `fw.outcome = "DELEGATED"`
  - `fw.operation_id = ...`
  - `fw.accepted_id = ...`
  - `fw.target_task = ...`
  - `fw.schedule_time = ...` (schedule only)
- Do not overload OTEL's built-in `SpanKind` enum with a custom string.

### 5. Keep feed publish as lineage, not as `DELEGATED`

Do not create a task-style delegation span for `RecordsFeed.publish()`.

Instead:

- continue using `RecordLinkKind.PUBLISHED` and `RecordLinkKind.CONSUMED`
- extend the stored record payload and the `RecordLink` schema with causal metadata
- let the consumer run reconstruct provenance from the consumed records rather than pretending a queue submission happened

Add a helper dataclass for record provenance if useful:

```python
@dataclass(frozen=True)
class RecordOriginMetadata:
    source_run_id: Optional[str]
    source_span_id: Optional[str]
    source_operation_id: Optional[str]
    source_feed_id: Optional[str]
    causation_event_id: Optional[str] = None
```

This keeps case B separate from case A while still making the graph queryable.

## Code Changes By Surface

### A. Context and correlation

Files:

- `packages/flowstash_lib/src/flowstash/context.py`
- `packages/flowstash_lib/src/flowstash/observability/model.py`

Changes:

- Extend `IntegrationContext` and `Correlation` with the causal fields listed above.
- Ensure `IntegrationContext.corelation` maps them into `Correlation`.
- Preserve these fields when nested in-process spans are created.

### B. Decorated task delegation

Files:

- `packages/flowstash_lib/src/flowstash/decorators.py`
- `packages/flowstash_lib/src/flowstash/queue/backend.py`

Changes:

- Replace `_enqueue_lifecycle(record_run_scheduled, ...)` in `TaskWrapper.submit()` and `TaskWrapper.schedule()`.
- Introduce a helper on `TaskWrapper`:

```python
def _delegate_task(
    self,
    *,
    mode: Literal["submit", "schedule"],
    eta_or_delay: Optional[Union[int, float, Any]] = None,
    args: tuple,
    kwargs: dict,
) -> JobHandle:
```

- Behavior of `_delegate_task(...)`:
  - If an active context exists, create a short-lived delegation span inside that run.
  - If no active context exists, open a real short-lived root `integration_context(...)` around the delegation accept call.
  - Start the span before calling the backend.
  - Call backend `submit()` / `schedule()`.
  - When the backend returns a handle, enrich the metadata with `job_id` and `scheduled_job_id`.
  - End the delegation span with `status="DELEGATED"` only after acceptance succeeds.
  - If acceptance fails, end the span with `status="ERROR"` and no child run link.

Backend interface change in `packages/flowstash_lib/src/flowstash/queue/backend.py`:

- Add `delegation: Optional[TaskDelegationMetadata] = None` to both `TaskBackend.submit(...)` and `TaskBackend.schedule(...)`.

### C. Managed task backend

File:

- `packages/flowstash_lib/src/flowstash/queue/backends/managed_tasks.py`

Changes:

- Do not serialize the trigger run as the future execution `run_id`.
- Add `delegation` into `payload`:

```python
payload["payload"]["delegation"] = asdict(delegation)
```

- Set `accepted_id` on the metadata from the `task_id` returned by the managed API; this becomes the stable join key between the delegation span and the child run.
- For cron-scheduled tasks, store the backend-specific job identity in `attrs["scheduled_job_id"]` rather than as a promoted field.

### D. Asyncio backend

File:

- `packages/flowstash_lib/src/flowstash/queue/asyncio_backend.py`

Changes:

- Change `.submit()` semantics to simulate remote delegation semantics rather than inherited same-run execution.
- Create a fresh run when the submitted task actually starts.
- Pass causal metadata from `delegation` into the new execution context.
- Copy `operation_id` from the submit span into the child execution run so the UI can resolve the link even before any explicit parent-child run table exists.

Important behavior split:

- `.run()` stays immediate and in-process.
- `.submit()` becomes a delegated execution even in local async mode.

This will require updating tests that currently expect submitted nested tasks to remain spans inside the parent's run.

### E. Dramatiq backend

File:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/dramatiq/dramatiq_backend.py`

Changes:

- Extend `_prepare_headers(...)` to include the new delegation metadata.
- Middleware should create a new execution `run_id` for delegated work.
- Middleware should preserve `parent_run_id` and `operation_id` in the new `IntegrationContext`.
- Keep using manual lifecycle recording in middleware, but make the lifecycle belong to the new execution run.

### F. Managed HTTP worker task execution

File:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`

Changes:

- Update `TaskPayload`:

```python
class TaskPayload(BaseModel):
    ...
    delegation: Optional[DelegationMetadataModel] = None
```

- `handle_task()` should:
  - allocate a fresh `run_id` for the execution attempt
  - place `parent_run_id` and `operation_id` from `payload.delegation` onto the execution context
  - stop treating incoming `run_id` as the execution run id

Suggested helper:

```python
def _build_execution_context_from_payload(
    payload: TaskPayload,
) -> dict[str, Any]:
```

### G. Feed publish and downstream record lineage

Files:

- `packages/flowstash_lib/src/flowstash/pipelines/records_feed.py`
- `packages/flowstash_lib/src/flowstash/pipelines/backends/managed_feed.py`
- `packages/flowstash_lib/src/flowstash/pipelines/consumer.py`
- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`

Changes:

- Do not create a `DELEGATED` span for feed publish.
- On feed publish, attach origin metadata for each published record.
- Extend stored record payload with:
  - `source_run_id`
  - `source_span_id`
  - `source_operation_id`
  - `source_feed_id`
  - `causation_event_id`
- Extend `enqueue_record_link(...)` and `RecordLink` to optionally carry:
  - `source_run_id`
  - `source_span_id`
  - `source_operation_id`
  - `source_feed_id`
  - `causation_event_id`

Why extend `RecordLink` too:

- Feed publication and consumption are record-level lineage.
- A batched consumer run may process records from different trigger runs.
- The per-record `RecordLink` is the right place to preserve mixed provenance without forcing one fake delegation model onto the whole batch.
- In the UI, opening the consumer run should show the consumed record set and let the graph walk back to the producer run that published each record.

### H. Managed feed consumer barriers

File:

- `packages/flowstash_runtime/src/flowstash/runtime/worker/backends/managed/http_entrypoint.py`

Changes:

- `kick_batched()` should keep the execution run open until the `/ack` call succeeds.
- Recommended structure:
  - resolve lease
  - open `integration_context(...)`
  - run handler
  - post `/ack`
  - then allow the context to exit and record `RUN_ENDED`

- `deliver_classic()` should also be reviewed so the run ends after the handler path is fully committed to the HTTP response flow.

### I. Existing `record_run_scheduled()`

File:

- `packages/flowstash_lib/src/flowstash/observability/ingestion.py`

Recommendation:

- Deprecate `record_run_scheduled()` for task/feed delegation semantics.
- Either:
  - keep it only for scheduler-originated `SCHEDULED` run events, or
  - turn it into a thin compatibility wrapper that emits a task delegation span with `status="DELEGATED"` during transition.

## Observability Ingestion Layer Changes

These changes are required in the client library and in the managed ingestion API.

### Client-side ingestion changes in this repo

Files:

- `packages/flowstash_lib/src/flowstash/observability/model.py`
- `packages/flowstash_lib/src/flowstash/observability/ingestion.py`
- local store implementations under `packages/flowstash_lib/src/flowstash/observability/stores/`

Required client behavior:

- extend `Correlation` on all lifecycle events
- standardize task delegation span attrs so they are queryable and indexable
- extend `RecordLink` for record-level causal joins
- keep task-join metadata on child runs and on parent delegation spans

### Coordinated managed API changes outside this repo

Required contract additions:

- Extend task submit payload contract to accept and persist `delegation`
- Extend feed publish payload contract to accept and persist record-level causal metadata
- Extend feed lease / delivery payloads to return the causal metadata so consumers can attach it to consumed links and execution runs where applicable
- Update the UI/query model to join:
  - parent task span to child run by `operation_id` and `job_id`
  - child run to trigger run by `correlation.parent_run_id` and `correlation.trigger_run_id`
  - feed consumer run to producer run through consumed `RecordLink` and record origin metadata

Recommended UI behavior:

- On a run page, show task delegation spans inline in the run timeline with `DELEGATED` status and a link target of `job_id`.
- If the downstream task has executed, resolve that link to the child run using `operation_id` or `job_id`.
- On a consumer run page, show provenance through consumed records instead of a fake delegation span.

## Test Plan

### Update existing tests

- `packages/flowstash_lib/tests/test_observability_comprehensive.py`
  - update assumptions that submitted nested tasks execute as spans in the same run
  - new expectation: parent run gets a `DELEGATED` span and child execution gets a new run id
- `packages/flowstash_lib/tests/test_decorators.py`
  - assert task delegation metadata is passed to the backend and that the delegation span is marked `DELEGATED`
- `packages/flowstash_lib/tests/test_managed_backends.py`
  - assert managed payload includes `delegation`
  - assert future execution no longer reuses trigger `run_id`
  - assert `job_id` / `operation_id` become the stable join keys
- `packages/flowstash_lib/tests/test_observability_ingestion.py`
  - add coverage for task delegation span helpers and standardized attrs

### Add new tests

- `packages/flowstash_lib/tests/test_delegation_observability.py`
  - root `.submit()` creates a short-lived submission run and one `DELEGATED` span
  - in-run `.submit()` keeps the parent run short-lived but emits a `DELEGATED` span
  - `.schedule()` captures schedule metadata and `scheduled_job_id`
  - child execution run links back to the parent span by `operation_id` and `job_id`
  - feed publish emits provenance links, not a task-style delegation span

- `packages/flowstash_runtime/tests/test_runtime_wiring.py`
  - managed worker preserves causal metadata and allocates a new execution `run_id`

- Managed HTTP entrypoint tests
  - `kick_batched()` run ends only after `/ack`
  - classic delivery preserves single-record causal metadata

## Execution Order

1. Extend context and observability models with causal fields.
2. Add task delegation span helpers and transport metadata.
3. Change task backends and `TaskWrapper` to emit `DELEGATED` spans and propagate join keys.
4. Change managed and Dramatiq execution paths to allocate new execution run ids.
5. Extend feed publish and consume paths with record-level provenance metadata.
6. Move managed batched consumer run boundary to include `/ack`.
7. Update tests and local file-store fixtures.

## TODO

- [ ] Add `parent_run_id` and `operation_id` to `IntegrationContext` and `Correlation`.
- [ ] Add `TaskDelegationMetadata` and optionally `RecordOriginMetadata` to the observability model.
- [ ] Add task delegation span helpers in `ingestion.py`.
- [ ] Replace `record_run_scheduled()` usage in `TaskWrapper.submit()` and `TaskWrapper.schedule()`.
- [ ] Make root `.submit()` / `.schedule()` open a real short-lived run around delegation acceptance.
- [ ] Extend `TaskBackend.submit()` and `TaskBackend.schedule()` to accept task delegation metadata.
- [ ] Update `ManagedTasksBackend` payload shape so delegated execution gets a fresh `run_id` later.
- [ ] Update `AsyncioBackend.submit()` to model remote delegation semantics.
- [ ] Update Dramatiq headers and middleware to preserve causal metadata while creating a new execution run.
- [ ] Update managed `handle_task()` to create a fresh execution `run_id`.
- [ ] Extend feed publish payloads and `RecordLink` with provenance metadata.
- [ ] Keep `kick_batched()` run open through `/ack`.
- [ ] Add or update tests for task delegation, feed publication, managed worker execution, and local observability stores.

## Notes On Migration

- The least risky rollout is additive first:
  - add new fields to models and payloads
  - keep existing `RUN_SCHEDULED` events temporarily
  - teach the UI to recognize task spans with `fw.outcome = "DELEGATED"`
  - later demote `RUN_SCHEDULED` once downstream consumers no longer depend on it

- If querying parent spans by `operation_id` and `job_id` proves too expensive, a later phase can add a dedicated edge table or delegation store without changing the runtime semantics above.
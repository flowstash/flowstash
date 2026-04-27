"""
Comprehensive observability tests.

All tests use the file_observability_store and asyncio_backend fixtures from flowstash.testing.
No direct calls to record_run_started/ended/span_started/ended — lifecycle is driven
entirely by integration_context (via decorators, TaskWrapper.run(), and TaskWrapper.submit()).

File naming produced by LocalFileStore: <iso>_<run_id>_<type>.json
Use read_events(logs_dir, "RUN_STARTED") etc. to filter.
"""

import asyncio
import io
import json
import time
import pytest
from pathlib import Path
from unittest.mock import patch

from flowstash.config.observability_config import (
    ObservabilityConfig,
    StoreType,
    DurabilityMode,
)
from flowstash.context import integration_context, current_context
from flowstash.decorators import integration_step, integration_task
from flowstash.observability.ingestion import set_observability_config
from flowstash.observability import registry
from flowstash.observability.logging import logger
from flowstash.testing import file_observability_store, asyncio_backend, read_events

pytest_plugins = ["flowstash.testing"]


# ---------------------------------------------------------------------------
# Console / local file sanity tests (no lifecycle assertions, just smoke)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_observability_console():
    """Smoke test: CONSOLE store emits log lines."""
    config = ObservabilityConfig(
        store_type=StoreType.CONSOLE, durability=DurabilityMode.IMMEDIATE
    )
    set_observability_config(config)
    registry.configure(config)

    with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
        with integration_context(
            integration="parent-integration", tags={"env": "test"}
        ):
            logger.info("Parent log")
            with integration_context(integration_pipeline="child-pipeline"):
                logger.error("Child log error")

        time.sleep(0.1)
        output = mock_stdout.getvalue()

    assert "[OBSERVABILITY] Log: INFO - Parent log" in output
    assert "[OBSERVABILITY] Log: ERROR - Child log error" in output

    set_observability_config(ObservabilityConfig(store_type=StoreType.DISABLED))


# ---------------------------------------------------------------------------
# Core lifecycle tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_root_context_records_run(file_observability_store):
    """A bare root integration_context records exactly one RUN_STARTED and one RUN_ENDED."""
    logs_dir = file_observability_store

    with integration_context(integration="test", integration_pipeline="pipe"):
        pass

    # Allow executor threads to finish
    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    run_ended = read_events(logs_dir, "RUN_ENDED")
    span_started = read_events(logs_dir, "SPAN_STARTED")

    assert len(run_started) == 1, f"Expected 1 RUN_STARTED, got {len(run_started)}"
    assert len(run_ended) == 1, f"Expected 1 RUN_ENDED, got {len(run_ended)}"
    assert len(span_started) == 0, "Root context must not record a span"

    assert run_ended[0]["status"] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_nested_context_records_span(file_observability_store):
    """A nested integration_context records a span (not a run) with the same run_id."""
    logs_dir = file_observability_store

    with integration_context(
        integration="test", integration_pipeline="outer"
    ) as root_ctx:
        root_run_id = root_ctx.run_id
        with integration_context(integration_pipeline="inner", span_name="my_span"):
            pass

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    span_started = read_events(logs_dir, "SPAN_STARTED")
    span_ended = read_events(logs_dir, "SPAN_ENDED")

    assert len(run_started) == 1
    assert len(span_started) == 1
    assert len(span_ended) == 1

    assert span_started[0]["correlation"]["run_id"] == root_run_id
    assert span_started[0]["name"] == "my_span"

    # start_time must be populated on SPAN_STARTED
    assert (
        span_started[0]["start_time"] is not None
    ), "start_time must not be null on SPAN_STARTED"

    # parent_span_id must be the root context's span_id
    root_span_id = root_ctx.span_id
    assert span_started[0]["correlation"]["parent_span_id"] == root_span_id, (
        f"parent_span_id {span_started[0]['correlation']['parent_span_id']!r} "
        f"!= root span_id {root_span_id!r}"
    )


@pytest.mark.asyncio
async def test_root_context_failed_records_failed_status(file_observability_store):
    """Exceptions inside a root context record RUN_ENDED with FAILED status."""
    logs_dir = file_observability_store

    with pytest.raises(ValueError):
        with integration_context(integration="test", integration_pipeline="pipe"):
            raise ValueError("boom")

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_ended = read_events(logs_dir, "RUN_ENDED")
    assert len(run_ended) == 1
    assert run_ended[0]["status"] == "FAILED"


# ---------------------------------------------------------------------------
# Decorator tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_integration_step_root_records_run(file_observability_store):
    """@integration_step on a root call records a run; span_name defaults to func.__name__."""
    logs_dir = file_observability_store

    @integration_step(integration="test", integration_pipeline="pipe")
    async def my_step():
        return "ok"

    result = await my_step()
    assert result == "ok"

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    assert len(read_events(logs_dir, "RUN_STARTED")) == 1
    assert len(read_events(logs_dir, "RUN_ENDED")) == 1
    assert len(read_events(logs_dir, "SPAN_STARTED")) == 0


@pytest.mark.asyncio
async def test_integration_step_nested_records_span(file_observability_store):
    """@integration_step inside a parent context records a span with the function name."""
    logs_dir = file_observability_store

    @integration_step(integration="test", integration_pipeline="inner")
    async def inner_step():
        pass

    with integration_context(integration="test", integration_pipeline="outer"):
        await inner_step()

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    span_started = read_events(logs_dir, "SPAN_STARTED")

    assert len(run_started) == 1, "Outer context opens the run"
    assert len(span_started) == 1, "inner_step should record a span"
    assert span_started[0]["name"] == "inner_step"
    assert (
        span_started[0]["start_time"] is not None
    ), "start_time must not be null on nested @integration_step span"


@pytest.mark.asyncio
async def test_integration_step_custom_span_name(file_observability_store):
    """span_name parameter overrides the default function name."""
    logs_dir = file_observability_store

    @integration_step(
        integration="test", integration_pipeline="pipe", name="custom_name"
    )
    async def ignored_name():
        pass

    with integration_context(integration="test", integration_pipeline="outer"):
        await ignored_name()

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    span_started = read_events(logs_dir, "SPAN_STARTED")
    assert len(span_started) == 1
    assert span_started[0]["name"] == "custom_name"


@pytest.mark.asyncio
async def test_integration_task_run_records_run(file_observability_store):
    """TaskWrapper.run() on a root call records a run."""
    logs_dir = file_observability_store

    @integration_task(integration="test", integration_pipeline="pipe")
    async def my_task():
        return 42

    result = await my_task.run()
    assert result == 42

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    assert len(read_events(logs_dir, "RUN_STARTED")) == 1
    assert len(read_events(logs_dir, "RUN_ENDED")) == 1
    assert len(read_events(logs_dir, "SPAN_STARTED")) == 0


# ---------------------------------------------------------------------------
# AsyncioBackend / run_id propagation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_submit_root_records_run(file_observability_store, asyncio_backend):
    """A root task submitted via AsyncioBackend records one run (not a span)."""
    logs_dir = file_observability_store

    @integration_task(integration="test", integration_pipeline="pipe")
    async def my_task():
        pass

    handle = my_task.submit()
    await handle.result(timeout=2.0)

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    # submit() itself records a RUN_SCHEDULED, then execution records RUN_STARTED/ENDED
    assert len(read_events(logs_dir, "RUN_STARTED")) == 1
    assert len(read_events(logs_dir, "RUN_ENDED")) == 1
    assert len(read_events(logs_dir, "SPAN_STARTED")) == 0


@pytest.mark.asyncio
async def test_task_submit_from_run_records_span_with_same_run_id(
    file_observability_store, asyncio_backend
):
    """A task submitted from inside a run records a span that shares the parent run_id."""
    logs_dir = file_observability_store

    @integration_task(integration="test", integration_pipeline="worker")
    async def subtask():
        pass

    with integration_context(
        integration="test", integration_pipeline="root"
    ) as root_ctx:
        root_run_id = root_ctx.run_id
        handle = subtask.submit()
        await handle.result(timeout=2.0)

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    span_started = read_events(logs_dir, "SPAN_STARTED")
    span_ended = read_events(logs_dir, "SPAN_ENDED")

    assert len(run_started) == 1, "Only the outer context should open a run"
    assert len(span_started) == 1, "subtask should be recorded as a span"
    assert len(span_ended) == 1

    assert (
        span_started[0]["correlation"]["run_id"] == root_run_id
    ), "Span must share run_id with the parent run"
    assert span_started[0]["name"] == "subtask"
    assert (
        span_started[0]["start_time"] is not None
    ), "start_time must not be null on submitted subtask span"
    assert (
        span_started[0]["correlation"]["parent_span_id"] is not None
    ), "parent_span_id must be set on submitted subtask span"


@pytest.mark.asyncio
async def test_nested_task_run_id_propagation(
    file_observability_store, asyncio_backend
):
    """Run ID is consistent across root run and all nested task spans."""
    logs_dir = file_observability_store

    @integration_task(integration="test", integration_pipeline="worker")
    async def leaf_task():
        pass

    @integration_task(integration="test", integration_pipeline="middle")
    async def middle_task():
        # Submit from inside another task — should still be a span
        h = leaf_task.submit()
        await h.result(timeout=2.0)

    handle = middle_task.submit()
    await handle.result(timeout=2.0)
    # Give leaf_task a moment to complete
    await asyncio.sleep(0.05)

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    span_started = read_events(logs_dir, "SPAN_STARTED")

    assert len(run_started) == 1, "Exactly one root run"
    assert len(span_started) == 1, "leaf_task submitted from middle_task is a span"

    root_run_id = run_started[0]["correlation"]["run_id"]
    for ev in span_started:
        assert (
            ev["correlation"]["run_id"] == root_run_id
        ), f"Span run_id {ev['correlation']['run_id']} != root {root_run_id}"


@pytest.mark.asyncio
async def test_run_id_preserved_across_task_and_step(
    file_observability_store, asyncio_backend
):
    """run_id is the same for a root task and an @integration_step called inside it."""
    logs_dir = file_observability_store

    @integration_step(integration="test", integration_pipeline="pipe")
    async def inner_step():
        pass

    @integration_task(integration="test", integration_pipeline="root")
    async def root_task():
        await inner_step()

    handle = root_task.submit()
    await handle.result(timeout=2.0)

    from flowstash.observability.ingestion import AsyncManager

    AsyncManager.get_instance().flush(timeout=5.0)

    run_started = read_events(logs_dir, "RUN_STARTED")
    span_started = read_events(logs_dir, "SPAN_STARTED")

    assert len(run_started) == 1
    assert len(span_started) == 1

    root_run_id = run_started[0]["correlation"]["run_id"]
    assert span_started[0]["correlation"]["run_id"] == root_run_id
    assert span_started[0]["name"] == "inner_step"


if __name__ == "__main__":
    pytest.main([__file__])

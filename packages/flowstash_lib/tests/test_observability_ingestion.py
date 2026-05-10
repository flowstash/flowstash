import pytest
import asyncio
from unittest.mock import MagicMock, patch
from datetime import datetime
from flowstash.context import integration_context
from flowstash.observability.ingestion import (
    AsyncManager,
    record_run_started,
    record_run_ended,
    record_span_started,
    record_span_ended,
    record_log,
    record_data_exchange,
    set_observability_config,
)
from flowstash.observability.model import Correlation, RunEvent, SpanEvent, DataExchangeEvent
from flowstash.config.observability_config import ObservabilityConfig, DurabilityMode
from flowstash.observability import registry


@pytest.fixture
def mock_stores():
    events_store = MagicMock()
    dx_store = MagicMock()
    blob_store = MagicMock()

    # Reset registry or use patch
    with (
        patch(
            "flowstash.observability.ingestion.get_events_store",
            return_value=events_store,
        ),
        patch(
            "flowstash.observability.ingestion.get_data_exchange_store",
            return_value=dx_store,
        ),
        patch(
            "flowstash.observability.ingestion.get_blob_store", return_value=blob_store
        ),
    ):
        yield events_store, dx_store, blob_store


@pytest.mark.asyncio
async def testrecord__started_immediate(mock_stores):
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ) as ctx:
        await record_run_started(artifact_id="art1")

    events_store.write_run_event.assert_called_once()
    event = events_store.write_run_event.call_args[0][0]
    assert isinstance(event, RunEvent)
    assert event.event_type == "STARTED"
    assert event.artifact_id == "art1"
    assert event.status == "RUNNING"
    assert event.correlation.integration == "app"


@pytest.mark.asyncio
async def testrecord__started_eventual(mock_stores):
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.EVENTUAL))

    with integration_context(record_lifecycle=False) as ctx:
        await record_run_started()

    # Wait for background task
    await asyncio.sleep(0.1)
    events_store.write_run_event.assert_called_once()


@pytest.mark.asyncio
async def test_eventual_execute_flush_waits_for_store_write():
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.EVENTUAL))
    manager = AsyncManager(max_workers=1)
    calls = []

    await manager.execute(lambda: calls.append("ran"))
    manager.flush(timeout=1.0)

    assert calls == ["ran"]


@pytest.mark.asyncio
async def test_record_data_exchange_with_payloads(mock_stores):
    _, dx_store, blob_store = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    blob_store.put.return_value = ("gs://ref", 10, "sha")

    # 1. Test inline path (offload_payloads=False by default)
    with integration_context() as ctx:
        await record_data_exchange(
            DataExchangeEvent(
                integration="sys",
                operation="op",
                channel="HTTP",
                address="http://api",
                request_payload=b"req",
                response_payload=b"res",
            )
        )

    assert blob_store.put.call_count == 0
    dx_store.write_data_exchange.assert_called_once()
    dx = dx_store.write_data_exchange.call_args[0][0]
    assert dx.request_payload == b"req"
    assert dx.response_payload == b"res"
    assert dx.request_size_bytes == 3
    assert dx.response_size_bytes == 3
    assert dx.request_payload_ref is None
    assert dx.response_payload_ref is None

    dx_store.write_data_exchange.reset_mock()

    # 2. Test offload path
    with integration_context() as ctx:
        await record_data_exchange(
            DataExchangeEvent(
                integration="sys",
                operation="op",
                channel="HTTP",
                address="http://api",
                request_payload=b"req",
                response_payload=b"res",
                offload_payloads=True,
            )
        )

    assert blob_store.put.call_count == 2
    dx_store.write_data_exchange.assert_called_once()
    dx = dx_store.write_data_exchange.call_args[0][0]
    assert dx.request_payload_ref == "gs://ref"
    assert dx.response_payload_ref == "gs://ref"
    assert dx.request_payload is None
    assert dx.response_payload is None


@pytest.mark.asyncio
async def test_error_suppression(mock_stores):
    events_store, _, _ = mock_stores
    # In ingestion.py, we use asyncio.to_thread for sync stores.
    # to_thread runs in another thread, so we should make sure the mock isn't broken.
    events_store.write_log.side_effect = Exception("Store failure")
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context():
        # This should NOT raise an exception
        await record_log("hello")

    events_store.write_log.assert_called_once()


# ─── metadata / attrs separation ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_started_attrs_and_metadata_are_stored_separately(mock_stores):
    """attrs (business data) and metadata (fw markers) must not bleed into each other."""
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ):
        await record_run_started(
            attrs={"user_id": "42"},
            metadata={"fw.span_kind": "task"},
        )

    events_store.write_run_event.assert_called_once()
    event: RunEvent = events_store.write_run_event.call_args[0][0]
    assert event.attrs == {"user_id": "42"}
    assert event.metadata == {"fw.span_kind": "task"}
    # fw marker must not leak into business attrs
    assert "fw.span_kind" not in event.attrs
    # business data must not leak into metadata
    assert "user_id" not in event.metadata


@pytest.mark.asyncio
async def test_run_ended_attrs_and_metadata_are_stored_separately(mock_stores):
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ):
        await record_run_ended(
            status="SUCCEEDED",
            attrs={"result_count": "5"},
            metadata={"fw.outcome": "OK"},
        )

    events_store.write_run_event.assert_called_once()
    event: RunEvent = events_store.write_run_event.call_args[0][0]
    assert event.attrs == {"result_count": "5"}
    assert event.metadata == {"fw.outcome": "OK"}
    assert "fw.outcome" not in event.attrs
    assert "result_count" not in event.metadata


@pytest.mark.asyncio
async def test_span_started_attrs_and_metadata_are_stored_separately(mock_stores):
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ):
        await record_span_started(
            name="my_step",
            attrs={"args": {"x": 1}},
            metadata={"fw.span_kind": "step"},
        )

    events_store.write_span_event.assert_called_once()
    event: SpanEvent = events_store.write_span_event.call_args[0][0]
    assert event.attrs == {"args": {"x": 1}}
    assert event.metadata == {"fw.span_kind": "step"}
    assert "fw.span_kind" not in event.attrs
    assert "args" not in event.metadata


@pytest.mark.asyncio
async def test_span_ended_attrs_and_metadata_are_stored_separately(mock_stores):
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ):
        await record_span_ended(
            name="my_step",
            status="OK",
            attrs={"result": "done"},
            metadata={"fw.span_kind": "step", "fw.outcome": "OK"},
        )

    events_store.write_span_event.assert_called_once()
    event: SpanEvent = events_store.write_span_event.call_args[0][0]
    assert event.attrs == {"result": "done"}
    assert event.metadata == {"fw.span_kind": "step", "fw.outcome": "OK"}
    assert "fw.span_kind" not in event.attrs
    assert "result" not in event.metadata


@pytest.mark.asyncio
async def test_run_started_attrs_default_to_empty(mock_stores):
    """Omitting attrs/metadata should produce empty dicts, not None."""
    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    with integration_context(
        integration="app", integration_pipeline="pipe", record_lifecycle=False
    ):
        await record_run_started()

    event: RunEvent = events_store.write_run_event.call_args[0][0]
    assert isinstance(event.attrs, dict)
    assert isinstance(event.metadata, dict)


@pytest.mark.asyncio
async def test_decorator_step_stores_args_in_attrs_and_span_kind_in_metadata(mock_stores):
    """@integration_step should put args in attrs and fw.span_kind in metadata.
    Must run inside an outer context so integration_context records a span (not a root run)."""
    from flowstash.decorators import integration_step

    events_store, _, _ = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    @integration_step(integration="test", integration_pipeline="pipe")
    async def my_step(user_id: str, amount: int):
        pass

    with integration_context(integration="test", integration_pipeline="pipe"):
        await my_step("u99", 100)

    # Flush the background lifecycle thread so all events are written before asserting
    AsyncManager.get_instance().flush(timeout=2.0)

    all_span_events = [c[0][0] for c in events_store.write_span_event.call_args_list]

    started = next((e for e in all_span_events if e.event_type == "STARTED"), None)
    ended = next((e for e in all_span_events if e.event_type == "ENDED"), None)

    assert started is not None, "Expected a STARTED span event"
    assert ended is not None, "Expected an ENDED span event"

    # STARTED: args in attrs, span_kind in metadata — no cross-contamination
    assert started.attrs.get("args") == {"user_id": "u99", "amount": 100}
    assert started.metadata.get("fw.span_kind") == "step"
    assert "fw.span_kind" not in started.attrs

    # ENDED: same attrs carried through, outcome added to metadata
    assert ended.attrs.get("args") == {"user_id": "u99", "amount": 100}
    assert ended.metadata.get("fw.span_kind") == "step"
    assert ended.metadata.get("fw.outcome") == "OK"
    assert "fw.span_kind" not in ended.attrs
    assert "fw.outcome" not in ended.attrs

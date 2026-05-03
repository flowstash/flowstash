import pytest
import asyncio
from unittest.mock import MagicMock, patch
from datetime import datetime
from flowstash.context import integration_context
from flowstash.observability.ingestion import (
    record_run_started,
    record_run_ended,
    record_log,
    record_data_exchange,
    set_observability_config,
)
from flowstash.observability.model import Correlation, RunEvent, DataExchangeEvent
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

    with integration_context(integration="app", integration_pipeline="pipe", record_lifecycle=False) as ctx:
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
async def test_record_data_exchange_with_payloads(mock_stores):
    _, dx_store, blob_store = mock_stores
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    blob_store.put.return_value = ("gs://ref", 10, "sha")

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

    assert blob_store.put.call_count == 2
    dx_store.write_data_exchange.assert_called_once()
    dx = dx_store.write_data_exchange.call_args[0][0]
    assert dx.request_payload_ref == "gs://ref"
    assert dx.response_payload_ref == "gs://ref"


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

import sys
from unittest.mock import MagicMock

# Mock flowstash.telemetry which seems to be missing from fs but used in http.py
mock_telemetry = MagicMock()
mock_otel = MagicMock()
mock_otel.get_tracer.return_value = MagicMock()
mock_telemetry.otel = mock_otel
sys.modules["flowstash.telemetry"] = mock_telemetry
sys.modules["flowstash.telemetry.otel"] = mock_otel

import pytest
import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from flowstash.clients import HttpClient, ClientSettings
from flowstash.config.observability_config import ObservabilityConfig, DurabilityMode
from flowstash.observability.ingestion import set_observability_config
from flowstash.observability import registry
from flowstash.observability.model import DataExchange, Correlation
from flowstash.observability.stores.protocols import DataExchangeStore, BlobStore


class MockDataExchangeStore(DataExchangeStore):
    def __init__(self):
        self.exchanges = []

    def write_data_exchange(self, dx: DataExchange) -> None:
        self.exchanges.append(dx)


class MockBlobStore(BlobStore):
    def __init__(self):
        self.blobs = {}  # path -> bytes

    def put(self, *, path_hint: str, content_type: str, data: bytes):
        self.blobs[path_hint] = data
        return f"mock://{path_hint}", len(data), "hash"


@pytest.mark.asyncio
async def test_http_client_observability_flow():
    # Setup Observability
    set_observability_config(ObservabilityConfig(durability=DurabilityMode.IMMEDIATE))

    # Setup Stores
    dx_store = MockDataExchangeStore()
    blob_store = MockBlobStore()
    registry.set_data_exchange_store(dx_store)
    registry.set_blob_store(blob_store)

    # Setup Client
    settings = ClientSettings(
        client_id="test-integration", baseUrl="https://api.example.com"
    )
    client = HttpClient(name="test-client", settings=settings)

    # Mock inner httpx client
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.text = '{"success": true}'
    mock_response.aread = AsyncMock(return_value=b'{"success": true}')
    mock_response.is_error = False

    client.client.request = AsyncMock(return_value=mock_response)

    # Execute
    await client.request("POST", "/test", json={"foo": "bar"})

    # Verify DataExchange
    assert len(dx_store.exchanges) == 1
    succeeded = dx_store.exchanges[0]

    assert succeeded.state == "SUCCEEDED"
    assert succeeded.integration == "test-integration"
    assert succeeded.operation == "POST /test"

    # For a plain JSON POST (no files), offload_payloads=False so payloads are
    # stored inline on the DataExchange object, not uploaded to the blob store.
    assert succeeded.request_payload == b'{"foo": "bar"}'
    assert succeeded.response_payload == b'{"success": true}'

    print("Observability verification pass!")

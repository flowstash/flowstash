import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
from flowstash.clients.http import HttpClient
from flowstash.clients.config import ClientSettings, RetryConfig


@pytest.fixture
def mock_record_data_exchange():
    """Mock the record_data_exchange function to verify it gets called."""
    with patch('flowstash.clients.http.record_data_exchange', new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def client_settings():
    """Create basic client settings for testing."""
    return ClientSettings(
        client_id="test_integration",
        baseUrl="https://api.example.com",
        retry=RetryConfig(max_retries=0)
    )


@pytest.mark.asyncio
async def test_http_client_records_data_exchange(respx_mock, client_settings, mock_record_data_exchange):
    """Test that HTTP client calls record_data_exchange for observability."""
    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200, json={"foo": "bar"})
    )
    
    client = HttpClient(name="EXAMPLE_API", settings=client_settings)
    
    response = await client.request("GET", "/test")
    assert response.status_code == 200
    
    # Verify record_data_exchange was called
    mock_record_data_exchange.assert_called_once()
    
    # Check the DataExchangeEvent was created with correct info
    call_args = mock_record_data_exchange.call_args
    event = call_args[0][0]  # First positional arg is the event
    
    assert event.integration == "test_integration"
    assert event.channel == "HTTP"
    assert event.operation == "GET /test"
    assert event.state == "SUCCEEDED"
    assert event.http_method == "GET"
    assert event.status_code == 200
    assert "api.example.com" in event.address

    await client.close()


@pytest.mark.asyncio
async def test_http_client_records_failed_exchange(respx_mock, client_settings, mock_record_data_exchange):
    """Test that HTTP client properly records failed requests."""
    respx_mock.get("https://api.example.com/error").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    
    client = HttpClient(name="EXAMPLE_API", settings=client_settings)
    
    with pytest.raises(httpx.HTTPStatusError):
        await client.request("GET", "/error")
    
    # Verify record_data_exchange was called with FAILED state
    mock_record_data_exchange.assert_called_once()
    event = mock_record_data_exchange.call_args[0][0]
    
    assert event.state == "FAILED"
    assert event.status_code == 500

    await client.close()


@pytest.mark.asyncio
async def test_http_client_no_otel_imports():
    """Verify HTTP client doesn't import OTEL directly."""
    import flowstash.clients.http as http_module
    
    # Check that opentelemetry is not in the module's namespace
    assert not hasattr(http_module, 'trace')
    assert not hasattr(http_module, 'get_tracer')
    assert not hasattr(http_module, 'inject')


def test_mask_sensitive_url_strips_api_key(client_settings):
    """_mask_sensitive_url replaces sensitive query-param values with ***."""
    client = HttpClient(name="TEST", settings=client_settings)

    url = "https://api.example.com/data?api_key=MY_SECRET&page=2"
    masked = client._mask_sensitive_url(url)

    assert "MY_SECRET" not in masked
    assert "api_key=***" in masked
    assert "page=2" in masked


def test_mask_sensitive_url_no_sensitive_params(client_settings):
    """_mask_sensitive_url leaves non-sensitive params unchanged."""
    client = HttpClient(name="TEST", settings=client_settings)

    url = "https://api.example.com/data?page=2&size=50"
    assert client._mask_sensitive_url(url) == url


def test_mask_sensitive_url_no_query(client_settings):
    """_mask_sensitive_url is a no-op when there are no query params."""
    client = HttpClient(name="TEST", settings=client_settings)

    url = "https://api.example.com/data"
    assert client._mask_sensitive_url(url) == url


def test_mask_sensitive_url_token_param(client_settings):
    """Values of params containing 'token' are masked."""
    client = HttpClient(name="TEST", settings=client_settings)

    url = "https://api.example.com/data?access_token=supersecret123&foo=bar"
    masked = client._mask_sensitive_url(url)

    assert "supersecret123" not in masked
    assert "foo=bar" in masked

import pytest
import httpx
from datetime import datetime, UTC, timedelta
from unittest.mock import AsyncMock, patch, MagicMock, call
from flowstash.clients.http import HttpClient, OAuth2Manager, _mask_oauth_payload
from flowstash.clients.config import (
    ClientSettings,
    ApiKeyAuthConfig,
    ApiKeyLocation,
    AuthType,
    OAuth2AuthConfig,
    RetryConfig,
)


@pytest.fixture
def client_settings_factory():
    def _create(auth_config):
        return ClientSettings(
            client_id="test_api", base_url="https://api.example.com", auth=auth_config
        )

    return _create


@pytest.mark.asyncio
async def test_api_key_header_auth(respx_mock, client_settings_factory):
    auth_config = ApiKeyAuthConfig(
        type=AuthType.API_KEY,
        key="X-API-Key",
        value="secret-key",
        in_=ApiKeyLocation.HEADER,
    )
    settings = client_settings_factory(auth_config)

    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )

    client = HttpClient(name="TEST", settings=settings)

    # Needs to patch record_data_exchange as well since it is called in request
    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        await client.request("GET", "/test")

    request = respx_mock.calls[0].request
    assert request.headers["X-API-Key"] == "secret-key"
    await client.close()


@pytest.mark.asyncio
async def test_api_key_query_auth(respx_mock, client_settings_factory):
    auth_config = ApiKeyAuthConfig(
        type=AuthType.API_KEY,
        key="api_key",
        value="secret-key",
        in_=ApiKeyLocation.QUERY,
    )
    settings = client_settings_factory(auth_config)

    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )

    client = HttpClient(name="TEST", settings=settings)

    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        await client.request("GET", "/test")

    request = respx_mock.calls[0].request
    url = request.url
    assert url.params["api_key"] == "secret-key"
    await client.close()


@pytest.mark.asyncio
async def test_oauth2_auth(respx_mock, client_settings_factory):
    auth_config = OAuth2AuthConfig(
        type=AuthType.OAUTH2,
        client_id="id",
        client_secret="secret",
        token_url="https://auth.example.com/token",
    )
    settings = client_settings_factory(auth_config)

    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )

    # Mock OAuth2Manager to avoid actual token request
    with patch("flowstash.clients.http.OAuth2Manager") as MockManager:
        mock_instance = MockManager.return_value
        mock_instance.get_token = AsyncMock(return_value="mock-token")

        client = HttpClient(name="TEST", settings=settings)

        with patch(
            "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
        ):
            await client.request("GET", "/test")

        request = respx_mock.calls[0].request
        assert request.headers["Authorization"] == "Bearer mock-token"

        await client.close()


class CustomHttpClient(HttpClient):
    async def authorize(self, headers, params, cookies):
        await super().authorize(headers, params, cookies)
        cookies["session_id"] = "123456"


@pytest.mark.asyncio
async def test_custom_auth_adds_cookies(respx_mock, client_settings_factory):
    # Use empty settings but valid structure
    settings = client_settings_factory(None)
    # The factory expects auth_config, if None passed it might fail in factory creation
    # Let's check factory implementation

    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )

    client = CustomHttpClient(name="TEST", settings=settings)

    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        await client.request("GET", "/test")

    request = respx_mock.calls[0].request
    # httpx handles cookie header formatting
    assert "session_id=123456" in request.headers["Cookie"]

    await client.close()


# ---------------------------------------------------------------------------
# OAuth2Manager unit tests
# ---------------------------------------------------------------------------


def _make_oauth_config(**kwargs):
    defaults = dict(
        client_id="my-client",
        client_secret="super-secret",
        token_url="https://auth.example.com/token",
    )
    defaults.update(kwargs)
    return OAuth2AuthConfig(**defaults)


@pytest.mark.asyncio
async def test_oauth2_token_cached_while_valid():
    """get_token() returns cached token without calling client.post when valid."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="svc")
    mgr._access_token = "cached-token"
    mgr._token_expires_at = datetime.now(UTC) + timedelta(hours=1)

    mock_client = MagicMock()
    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        token = await mgr.get_token(mock_client)

    assert token == "cached-token"
    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_oauth2_token_refreshed_when_expired():
    """get_token() calls refresh_token() when token is near expiry."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="svc")
    mgr._access_token = "old-token"
    # Expired 5 minutes ago
    mgr._token_expires_at = datetime.now(UTC) - timedelta(minutes=5)

    mock_resp = MagicMock()
    mock_resp.is_error = False
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "new-token", "expires_in": 3600}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        token = await mgr.get_token(mock_client)

    assert token == "new-token"
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_oauth2_token_expiry_parsed():
    """expires_in from token response populates _token_expires_at."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="svc")

    mock_resp = MagicMock()
    mock_resp.is_error = False
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "tok", "expires_in": 600}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    before = datetime.now(UTC)
    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        await mgr.refresh_token(mock_client)

    assert mgr._token_expires_at is not None
    # Should be ~10 minutes from now (600 - 30 buffer still in the future)
    assert mgr._token_expires_at > before + timedelta(seconds=500)


@pytest.mark.asyncio
async def test_oauth2_token_no_expiry_field():
    """When server omits expires_in, _token_expires_at stays None."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="svc")

    mock_resp = MagicMock()
    mock_resp.is_error = False
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "tok"}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("flowstash.clients.http.record_data_exchange", new_callable=AsyncMock):
        await mgr.refresh_token(mock_client)

    assert mgr._token_expires_at is None


@pytest.mark.asyncio
async def test_oauth2_refresh_records_data_exchange_success():
    """Happy-path refresh calls record_data_exchange with SUCCEEDED state."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="my-svc")

    mock_resp = MagicMock()
    mock_resp.is_error = False
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "tok", "expires_in": 3600}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        await mgr.refresh_token(mock_client)

    mock_dx.assert_called_once()
    event = mock_dx.call_args[0][0]
    assert event.operation == "OAUTH2_TOKEN_REFRESH"
    assert event.state == "SUCCEEDED"
    assert event.integration == "my-svc"
    assert event.address == config.token_url
    # Payload must be bytes and must NOT contain raw secret
    assert isinstance(event.request_payload, bytes)
    assert b"super-secret" not in event.request_payload


@pytest.mark.asyncio
async def test_oauth2_refresh_records_data_exchange_http_error():
    """4xx from token endpoint records FAILED and raises immediately (no retry)."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="my-svc")

    mock_resp = MagicMock()
    mock_resp.is_error = True
    mock_resp.status_code = 401
    mock_resp.text = "Unauthorized"
    mock_resp.request = MagicMock()
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        with pytest.raises(httpx.HTTPStatusError):
            await mgr.refresh_token(mock_client)

    # Called once — no retries for HTTP errors
    mock_dx.assert_called_once()
    event = mock_dx.call_args[0][0]
    assert event.state == "FAILED"
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_oauth2_refresh_retries_on_connect_error():
    """ConnectError triggers retry; succeeds on third attempt; records all attempts."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="my-svc")

    success_resp = MagicMock()
    success_resp.is_error = False
    success_resp.status_code = 200
    success_resp.json.return_value = {"access_token": "tok"}

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=[
            httpx.ConnectError("unreachable"),
            httpx.ConnectError("unreachable"),
            success_resp,
        ]
    )

    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        with patch("asyncio.sleep", new_callable=AsyncMock):  # skip actual waits
            token = await mgr.refresh_token(mock_client)

    assert token == "tok"
    assert mock_client.post.call_count == 3
    # 2 FAILED + 1 SUCCEEDED
    assert mock_dx.call_count == 3
    states = [c[0][0].state for c in mock_dx.call_args_list]
    assert states == ["FAILED", "FAILED", "SUCCEEDED"]


@pytest.mark.asyncio
async def test_oauth2_refresh_raises_after_max_retries():
    """ConnectError on all attempts raises after MAX_TOKEN_RETRIES."""
    from flowstash.clients.http import MAX_TOKEN_RETRIES

    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="my-svc")

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("down"))

    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.ConnectError):
                await mgr.refresh_token(mock_client)

    assert mock_client.post.call_count == MAX_TOKEN_RETRIES
    assert mock_dx.call_count == MAX_TOKEN_RETRIES


@pytest.mark.asyncio
async def test_oauth2_refresh_timeout_records_timeout_state():
    """TimeoutException on all retries records TIMEOUT state."""
    from flowstash.clients.http import MAX_TOKEN_RETRIES

    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="my-svc")

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.TimeoutException):
                await mgr.refresh_token(mock_client)

    states = [c[0][0].state for c in mock_dx.call_args_list]
    assert all(s == "TIMEOUT" for s in states)


@pytest.mark.asyncio
async def test_oauth2_invalidate_clears_expiry():
    """invalidate() clears both token and expiry."""
    config = _make_oauth_config()
    mgr = OAuth2Manager(config, integration_name="svc")
    mgr._access_token = "tok"
    mgr._token_expires_at = datetime.now(UTC) + timedelta(hours=1)

    mgr.invalidate()

    assert mgr._access_token is None
    assert mgr._token_expires_at is None


@pytest.mark.asyncio
async def test_authorize_failure_logged(respx_mock, client_settings_factory):
    """If OAuth2 token refresh fails, HttpClient.request() logs error before re-raising."""
    auth_config = OAuth2AuthConfig(
        client_id="id",
        client_secret="secret",
        token_url="https://auth.example.com/token",
    )
    settings = client_settings_factory(auth_config)
    client = HttpClient(name="MY_CLIENT", settings=settings)

    with patch.object(
        client._auth_manager,
        "get_token",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("token endpoint down"),
    ):
        with patch("flowstash.clients.http.logger") as mock_logger:
            with pytest.raises(httpx.ConnectError):
                await client.request("GET", "/data")

    error_calls = [str(c) for c in mock_logger.error.call_args_list]
    assert any("Authorization failed" in s and "MY_CLIENT" in s for s in error_calls)
    await client.close()


@pytest.mark.asyncio
async def test_api_key_query_secret_masked_in_observability(
    respx_mock, client_settings_factory
):
    """API-key-in-query value must not appear in record_data_exchange address."""
    auth_config = ApiKeyAuthConfig(
        key="api_key",
        value="MY_SUPER_SECRET",
        in_=ApiKeyLocation.QUERY,
    )
    settings = client_settings_factory(auth_config)

    respx_mock.get("https://api.example.com/data").mock(
        return_value=httpx.Response(200)
    )

    client = HttpClient(name="TEST", settings=settings)
    with patch(
        "flowstash.clients.http.record_data_exchange", new_callable=AsyncMock
    ) as mock_dx:
        await client.request("GET", "/data")

    event = mock_dx.call_args[0][0]
    assert "MY_SUPER_SECRET" not in event.address
    await client.close()


# ---------------------------------------------------------------------------
# _mask_oauth_payload unit test
# ---------------------------------------------------------------------------


def test_mask_oauth_payload_redacts_secrets():
    data = {
        "grant_type": "password",
        "username": "bob",
        "password": "hunter2",
        "client_id": "app",
        "client_secret": "abcdef1234",
        "refresh_token": "refresh-xyz",
    }
    result = _mask_oauth_payload(data)
    decoded = result.decode("utf-8")
    assert "hunter2" not in decoded
    assert "abcdef1234" not in decoded
    assert "refresh-xyz" not in decoded
    assert "grant_type=password" in decoded
    assert "username=bob" in decoded

import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
from flowstash.clients.http import HttpClient
from flowstash.clients.config import (
    ClientSettings,
    ApiKeyAuthConfig,
    ApiKeyLocation,
    AuthType,
    OAuth2AuthConfig
)

@pytest.fixture
def client_settings_factory():
    def _create(auth_config):
        return ClientSettings(
            client_id="test_api",
            base_url="https://api.example.com",
            auth=auth_config
        )
    return _create

@pytest.mark.asyncio
async def test_api_key_header_auth(respx_mock, client_settings_factory):
    auth_config = ApiKeyAuthConfig(
        type=AuthType.API_KEY,
        key="X-API-Key",
        value="secret-key",
        in_=ApiKeyLocation.HEADER
    )
    settings = client_settings_factory(auth_config)
    
    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )
    
    client = HttpClient(name="TEST", settings=settings)
    
    # Needs to patch record_data_exchange as well since it is called in request
    with patch('flowstash.clients.http.record_data_exchange', new_callable=AsyncMock):
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
        in_=ApiKeyLocation.QUERY
    )
    settings = client_settings_factory(auth_config)
    
    respx_mock.get("https://api.example.com/test").mock(
        return_value=httpx.Response(200)
    )
    
    client = HttpClient(name="TEST", settings=settings)
    
    with patch('flowstash.clients.http.record_data_exchange', new_callable=AsyncMock):
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
        token_url="https://auth.example.com/token"
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
        
        with patch('flowstash.clients.http.record_data_exchange', new_callable=AsyncMock):
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
    
    with patch('flowstash.clients.http.record_data_exchange', new_callable=AsyncMock):
        await client.request("GET", "/test")
    
    request = respx_mock.calls[0].request
    # httpx handles cookie header formatting
    assert "session_id=123456" in request.headers["Cookie"]
    
    await client.close()

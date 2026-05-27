import httpx
from typing import Optional, Dict, Any
from .config import load_global_config, get_access_token


class APIClient:
    def __init__(self, token: Optional[str] = None):
        self.config = load_global_config()
        self.base_url = self.config.api_url.rstrip("/")
        self._token = token  # explicit token takes priority over resolved one

    def _get_headers(self) -> Dict[str, str]:
        headers = {}
        token = self._token if self._token is not None else get_access_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None):
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.base_url}{path}", params=params, headers=self._get_headers()
            )
            response.raise_for_status()
            return response.json()

    async def post(self, path: str, json: Optional[Dict[str, Any]] = None, timeout: float = 30.0):
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{self.base_url}{path}", json=json, headers=self._get_headers()
            )
            response.raise_for_status()
            return response.json()

    async def put_binary(
        self, url: str, data: bytes, headers: Optional[Dict[str, str]] = None
    ):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.put(url, content=data, headers=headers or {})
            response.raise_for_status()
            return response

    async def delete(self, path: str):
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.delete(
                f"{self.base_url}{path}", headers=self._get_headers()
            )
            response.raise_for_status()
            # 204 No Content returns no body
            if response.status_code == 204:
                return {}
            return response.json()

    # Generic request method if needed
    async def request(self, method: str, path: str, **kwargs):
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.base_url}{path}"
            headers = self._get_headers()
            if "headers" in kwargs:
                headers.update(kwargs.pop("headers"))

            response = await client.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json()

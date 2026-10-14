import httpx
import os
from urllib.parse import quote
from typing import Optional, Dict, Any
from .config import load_global_config, get_access_token


class APIClient:
    def __init__(self, token: Optional[str] = None):
        self.config = load_global_config()
        self.base_url = os.getenv("FLOWSTASH_API_URL", self.config.api_url).rstrip("/")
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

    async def diagnostic_query(self, resource: str, params: Dict[str, Any], run_id: Optional[str] = None):
        path = f"/observability/{resource}"
        if run_id is not None:
            suffix = "events" if resource == "run-events" else resource
            path = f"/observability/runs/{quote(run_id, safe='')}/{suffix}"
        return await self.get(path, params=params)

    async def diagnostic_run(self, run_id: str, params: Dict[str, Any]):
        return await self.get(f"/observability/runs/{quote(run_id, safe='')}", params=params)

    async def diagnostic_payload(self, ref: str, params: Dict[str, Any]):
        return await self.get("/observability/blobs", params={**params, "ref": ref})

    async def build_status(self, build_id: str):
        return await self.get(f"/v1/builds/{quote(build_id, safe='')}/status")

    async def build_log_entries(self, build_id: str, params: Dict[str, Any]):
        return await self.get(f"/v1/builds/{quote(build_id, safe='')}/log-entries", params=params)

    async def post(
        self, path: str, json: Optional[Dict[str, Any]] = None, timeout: float = 30.0
    ):
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

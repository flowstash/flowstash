from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Dict, AsyncIterator, List, Optional, TypeVar

from .http import HttpClient

T = TypeVar("T")

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ODataPage:
    """
    Holds a page of OData results.
    """
    value: List[Dict[str, Any]]
    next_link: Optional[str] = None
    delta_link: Optional[str] = None
    count: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class ODataError(Exception):
    """
    Structured OData error.
    """
    http_status: int
    message: str
    code: Optional[str] = None
    details: Any = None
    request_id: Optional[str] = None

    def __str__(self) -> str:
        c = f" ({self.code})" if self.code else ""
        return f"ODataError{c}: {self.http_status} {self.message}"

class ODataHttpError(Exception):
    """
    Exception raised when an OData request fails.
    """
    def __init__(self, err: ODataError):
        super().__init__(str(err))
        self.err = err

class ODataQuery:
    """
    Helper to build OData query options.
    """
    def __init__(self) -> None:
        self._params: Dict[str, str] = {}

    def select(self, *fields: str) -> "ODataQuery":
        self._params["$select"] = ",".join(fields)
        return self

    def filter(self, expr: str) -> "ODataQuery":
        self._params["$filter"] = expr
        return self

    def expand(self, expr: str) -> "ODataQuery":
        self._params["$expand"] = expr
        return self

    def orderby(self, expr: str) -> "ODataQuery":
        self._params["$orderby"] = expr
        return self

    def top(self, n: int) -> "ODataQuery":
        self._params["$top"] = str(n)
        return self

    def skip(self, n: int) -> "ODataQuery":
        self._params["$skip"] = str(n)
        return self

    def count(self, enabled: bool = True) -> "ODataQuery":
        self._params["$count"] = "true" if enabled else "false"
        return self

    def to_params(self) -> Dict[str, str]:
        return dict(self._params)

class ODataClient:
    """
    OData client wrapping HttpClient.
    Provides ergonomic access to OData services.
    """
    def __init__(self, http: HttpClient, *, default_headers: Optional[Dict[str, str]] = None):
        self._http = http
        self._default_headers = {"Accept": "application/json"}
        if default_headers:
            self._default_headers.update(default_headers)

    async def list(
        self, 
        entity_set: str, 
        query: Optional[ODataQuery] = None, 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> ODataPage:
        """
        List entities in a set.
        """
        return await self._get_page(
            entity_set, 
            params=(query.to_params() if query else None), 
            headers=headers
        )

    async def iter(
        self, 
        entity_set: str, 
        query: Optional[ODataQuery] = None, 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Iterate over all entities in a set, following next links automatically.
        """
        page = await self.list(entity_set, query, headers=headers)
        for item in page.value:
            yield item
        
        while page.next_link:
            page = await self.get_by_link(page.next_link, headers=headers)
            for item in page.value:
                yield item

    async def get(
        self, 
        entity_set: str, 
        key: str, 
        query: Optional[ODataQuery] = None, 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Get a single entity by key.
        """
        # Note: key might need quotes etc. depending on server, caller should handle it or we can try to be smart.
        # Minimalist approach: let caller provide it exactly.
        path = f"{entity_set.strip('/')}({key})"
        return await self._request_json(
            "GET", 
            path, 
            params=(query.to_params() if query else None), 
            headers=headers
        )

    async def create(
        self, 
        entity_set: str, 
        body: Dict[str, Any], 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new entity.
        """
        path = f"/{entity_set.lstrip('/')}"
        return await self._request_json("POST", path, json=body, headers=headers)

    async def update(
        self, 
        entity_set: str, 
        key: str, 
        body: Dict[str, Any], 
        *, 
        etag: Optional[str] = None, 
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Update an entity via PATCH.
        """
        path = f"{entity_set.strip('/')}({key})"
        h = dict(headers or {})
        if etag:
            h["If-Match"] = etag
        return await self._request_json("PATCH", path, json=body, headers=h)

    async def delete(
        self, 
        entity_set: str, 
        key: str, 
        *, 
        etag: Optional[str] = None, 
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Delete an entity.
        """
        path = f"{entity_set.strip('/')}({key})"
        h = dict(headers or {})
        if etag:
            h["If-Match"] = etag
        await self._request_no_content("DELETE", path, headers=h)

    async def get_by_link(self, next_or_delta_link: str, *, headers: Optional[Dict[str, str]] = None) -> ODataPage:
        """
        Fetch a page using an opaque nextLink or deltaLink.
        """
        url = next_or_delta_link
        base_url = self._http.base_url
        
        if url.startswith(base_url):
            url = url[len(base_url):]
        
        return await self._get_page(url, params=None, headers=headers)

    async def invoke_function(
        self, 
        path: str, 
        params: Optional[Dict[str, Any]] = None, 
        query: Optional[ODataQuery] = None,
        *,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Invoke an OData function (GET).
        """
        all_params = dict(params or {})
        if query:
            all_params.update(query.to_params())
            
        return await self._request_json(
            "GET", 
            path, 
            params=all_params,
            headers=headers
        )

    async def invoke_action(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None,
        *,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Invoke an OData action (POST).
        """
        return await self._request_json("POST", path, json=body, headers=headers)

    # -------- internal helpers --------

    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        merged = dict(self._default_headers)
        if headers:
            merged.update(headers)
        return merged

    async def _get_page(self, path: str, *, params: Optional[Dict[str, str]], headers: Optional[Dict[str, str]]) -> ODataPage:
        payload = await self._request_json("GET", path, params=params, headers=headers)
        value = payload.get("value", [])
        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        count = payload.get("@odata.count")
        return ODataPage(
            value=value,
            next_link=next_link,
            delta_link=delta_link,
            count=count,
            raw=payload,
        )

    async def _request_json(
        self, 
        method: str, 
        path: str, 
        *, 
        params: Optional[Dict[str, str]] = None,
        json: Any = None, 
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        try:
            resp = await self._http.request(
                method, 
                path, 
                headers=self._merge_headers(headers), 
                params=params, 
                json=json
            )
            # Some endpoints may return 204 for PATCH/DELETE; handle separately if needed
            if resp.status_code == 204 or not resp.text:
                return {}
            return resp.json()
        except Exception as e:
             # HttpClient.request already handles error status and includes response body in the exception.
             # But it doesn't parse OData specific error bodies into ODataError.
             # We might need to wrap the call and catch HTTPStatusError if we want OData-specific errors.
             import httpx
             if isinstance(e, httpx.HTTPStatusError):
                 raise ODataHttpError(await self._parse_error(e.response))
             raise e

    async def _request_no_content(self, method: str, url: str, *, headers: Optional[Dict[str, str]] = None) -> None:
        try:
            await self._http.request(method, url, headers=self._merge_headers(headers))
        except Exception as e:
            import httpx
            if isinstance(e, httpx.HTTPStatusError):
                raise ODataHttpError(await self._parse_error(e.response))
            raise e

    async def _parse_error(self, resp: Any) -> ODataError:
        request_id = None
        try:
            request_id = resp.headers.get("request-id") or resp.headers.get("x-ms-request-id")
        except Exception:
            pass

        message = getattr(resp, "text", "") or "HTTP error"
        code = None
        details = None

        try:
            data = resp.json()
            if isinstance(data, dict) and "error" in data and isinstance(data["error"], dict):
                err = data["error"]
                code = err.get("code")
                # message can be string or object depending on server
                msg = err.get("message")
                if isinstance(msg, dict):
                    message = msg.get("value") or message
                elif isinstance(msg, str):
                    message = msg
                details = err.get("innererror") or err.get("details") or err
            else:
                details = data
        except Exception:
            # fall back to text
            pass

        return ODataError(
            http_status=int(resp.status_code),
            message=str(message),
            code=code,
            details=details,
            request_id=request_id,
        )

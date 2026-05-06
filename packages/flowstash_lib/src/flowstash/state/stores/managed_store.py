from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from ..entry import StateEntry
from ..protocol import _validate, _decode_entry


def _epoch_to_dt(epoch: Optional[float]) -> Optional[datetime]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class ManagedStateStore:
    """
    StateStore that talks to a remote managed state service over HTTP.

    Endpoint: ``{base_url}/state``

    Auth: Bearer token from ``api_key`` (or flowstash_API_KEY env var).

    Value transport: base64-encoded bytes in JSON body.
    """

    _MAX_RETRIES = 3
    _RETRY_DELAY_S = 0.5

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_s: float = 5.0,
        http_client: Optional[httpx.Client] = None,
    ):
        import os

        self._base_url = (
            base_url or os.environ.get("FLOWSTASH_API_URL", "https://api.flowstash.dev")
        ).rstrip("/")
        self._api_key = api_key or os.environ.get("FLOWSTASH_API_KEY", "")
        self._timeout = timeout_s
        self._http = http_client or httpx.Client(timeout=timeout_s)

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self._api_key:
            h["X-API-Key"] = f"{self._api_key}"
        return h

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self._base_url}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(self._MAX_RETRIES):
            try:
                resp = self._http.request(
                    method, url, headers=self._headers(), **kwargs
                )
                if resp.status_code in (429,) or resp.status_code >= 500:
                    time.sleep(self._RETRY_DELAY_S * (2**attempt))
                    continue
                return resp
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                time.sleep(self._RETRY_DELAY_S * (2**attempt))
        params = kwargs.get("params", {})
        raise RuntimeError(
            f"ManagedStateStore: request {method} {url}  failed after {self._MAX_RETRIES} retries "
            f"for params={params}"
        ) from last_exc

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    def get_entry(self, namespace: str, key: str) -> Optional[StateEntry]:
        _validate(namespace, key)
        resp = self._request(
            "GET", "/state", params={"namespace": namespace, "key": key}
        )
        if resp.status_code == 404:
            return None
        if resp.status_code == 401 or resp.status_code == 403:
            raise PermissionError(f"ManagedStateStore: auth error {resp.status_code}")
        resp.raise_for_status()

        body = resp.json()
        raw = base64.b64decode(body.get("value")) if body.get("value") else b""
        return StateEntry(
            value=raw,
            content_type=body.get("content_type", "application/json"),
            updated_at=_epoch_to_dt(body.get("updated_at")),
            expires_at=_epoch_to_dt(body.get("expires_at")),
            version=body.get("version", 1),
        )

    def get(self, namespace: str, key: str) -> Optional[Any]:
        entry = self.get_entry(namespace, key)
        if entry is None:
            return None
        return _decode_entry(entry)

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        content_type: str = "application/json",
        ttl_s: Optional[int] = None,
    ) -> StateEntry:
        _validate(namespace, key)

        if isinstance(value, (dict, list)):
            raw = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            raw = value.encode("utf-8")
        elif isinstance(value, bytes):
            raw = value
        else:
            raw = json.dumps(value).encode("utf-8")

        payload: dict = {
            "namespace": namespace,
            "key": key,
            "value": base64.b64encode(raw).decode("ascii"),
            "content_type": content_type,
        }
        if ttl_s is not None:
            payload["ttl_s"] = ttl_s

        resp = self._request("PUT", "/state", content=json.dumps(payload))
        if resp.status_code in (401, 403):
            raise PermissionError(f"ManagedStateStore: auth error {resp.status_code}")
        resp.raise_for_status()

        body = resp.json()
        stored_raw = base64.b64decode(body.get("value")) if body.get("value") else b""
        return StateEntry(
            value=stored_raw,
            content_type=body.get("content_type", content_type),
            updated_at=_epoch_to_dt(body.get("updated_at")),
            expires_at=_epoch_to_dt(body.get("expires_at")),
            version=body.get("version", 1),
        )

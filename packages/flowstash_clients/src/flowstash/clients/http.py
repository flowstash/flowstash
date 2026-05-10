from .base import BaseClient
from .registry import get_client
import httpx
import logging
import asyncio
import datetime
import fnmatch
import re
from pathlib import Path
from datetime import UTC, timedelta, datetime
from typing import Any, Optional, Dict, List, Union
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import weakref

from .config import (
    ClientSettings,
    AuthConfig,
    AuthType,
    BasicAuthConfig,
    ApiKeyAuthConfig,
    ApiKeyLocation,
    OAuth2AuthConfig,
    OAuth2ClientAuthMethod,
    TLSConfig,
    Behaviour,
)

# Observability imports from flowstash
from flowstash.observability.model import DataExchangeEvent
from flowstash.observability.ingestion import record_data_exchange

logger = logging.getLogger(__name__)

# Max retries for transient failures on the OAuth2 token endpoint
MAX_TOKEN_RETRIES = 3

# Query-param key substrings considered sensitive — values are masked in URLs
_SENSITIVE_PARAM_KEYS = frozenset(
    [
        "api_key",
        "apikey",
        "key",
        "token",
        "secret",
        "password",
        "access_token",
        "client_secret",
    ]
)

# Header keys whose values are NOT masked in DataExchangeEvents
_UNMASKED_HEADER_KEYS = frozenset(["content-type"])


def _mask_value(val: str) -> str:
    """Mask a value: show first 3 and last 3 chars, hide the rest.
    Falls back to *** for short values (<=6 chars).
    """
    if not val or len(val) <= 6:
        return "***"
    return f"{val[:3]}...{val[-3:]}"


def _mask_oauth_payload(data: Dict[str, Any]) -> bytes:
    """Return URL-encoded bytes of *data* with credential values redacted."""
    import shlex

    def _mask(val: str) -> str:
        if not val or len(val) <= 4:
            return "***"
        return f"***{val[-4:]}"

    masked = {}
    for k, v in data.items():
        if any(
            s in k.lower() for s in ("secret", "password", "refresh_token", "token")
        ):
            masked[k] = _mask(str(v))
        else:
            masked[k] = v
    return urlencode(masked).encode("utf-8")


class SuppressedEndpointError(httpx.HTTPError):
    """Raised when a request is suppressed by traffic governance rules."""

    def __init__(self, method: str, url: str):
        super().__init__(f"Request suppressed by governance: {method} {url}")


class OAuth2Manager:
    """
    OAuth2 manager for token handling, expiry tracking, and re-auth.
    Token endpoint calls are retried on transient network errors and
    recorded via record_data_exchange (with credentials masked).
    """

    def __init__(self, config: OAuth2AuthConfig, integration_name: str = ""):
        self.config = config
        self.integration_name = integration_name or config.client_id
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._expiry_buffer_seconds: int = 30
        self._lock = asyncio.Lock()

    def _is_token_valid(self) -> bool:
        if not self._access_token:
            return False
        if self._token_expires_at is None:
            # No expiry info from server — assume valid until a 401 tells us otherwise
            return True
        return datetime.now(UTC) < (
            self._token_expires_at - timedelta(seconds=self._expiry_buffer_seconds)
        )

    async def get_token(self, client: httpx.AsyncClient) -> str:
        async with self._lock:
            if self._is_token_valid():
                return self._access_token
            return await self.refresh_token(client)

    async def refresh_token(self, client: httpx.AsyncClient) -> str:
        logger.info(
            f"Refreshing OAuth2 token for {self.config.client_id} via {self.config.token_url}"
        )

        # Build request data
        grant_type = self.config.grant_type
        if self.config.refresh_token:
            grant_type = "refresh_token"

        data: Dict[str, Any] = {"grant_type": grant_type}

        if grant_type == "password":
            if self.config.username:
                data["username"] = self.config.username
            if self.config.password:
                data["password"] = self.config.password
        elif grant_type == "refresh_token":
            if self.config.refresh_token:
                data["refresh_token"] = self.config.refresh_token
        elif grant_type == "client_credentials":
            pass

        if self.config.scopes:
            data["scope"] = " ".join(self.config.scopes)

        data.update(self.config.extra_params)

        if self.config.client_auth_method == OAuth2ClientAuthMethod.CLIENT_SECRET_BASIC:
            post_kwargs: Dict[str, Any] = {
                "auth": (self.config.client_id, self.config.client_secret)
            }
        else:
            data["client_id"] = self.config.client_id
            data["client_secret"] = self.config.client_secret
            post_kwargs = {}

        masked_payload = _mask_oauth_payload(data)

        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_TOKEN_RETRIES + 1):
            started_at = datetime.now(UTC)
            response: Optional[httpx.Response] = None
            state = "FAILED"
            exc_for_record: Optional[Exception] = None

            try:
                response = await client.post(
                    self.config.token_url, data=data, **post_kwargs
                )
                completed_at = datetime.now(UTC)

                if response.is_error:
                    body = response.text
                    state = "FAILED"

                    # Build curl hint (masked)
                    import shlex as _shlex

                    curl_cmd = "<Error generating curl command>"
                    try:
                        curl_cmd = (
                            f"curl -v -X POST {_shlex.quote(self.config.token_url)}"
                            f" -d {_shlex.quote(masked_payload.decode('utf-8', errors='replace'))}"
                        )
                    except Exception:
                        pass

                    err = httpx.HTTPStatusError(
                        f"OAuth2 token refresh failed with status {response.status_code}."
                        f" Response: {body}\nReplicate with:\n{curl_cmd}",
                        request=response.request,
                        response=response,
                    )
                    logger.error(
                        f"OAuth2 token refresh error for {self.integration_name}"
                        f" (attempt {attempt}/{MAX_TOKEN_RETRIES}):"
                        f" HTTP {response.status_code} from {self.config.token_url}"
                    )
                    exc_for_record = err

                    await record_data_exchange(
                        DataExchangeEvent(
                            integration=self.integration_name,
                            channel="HTTP",
                            operation="OAUTH2_TOKEN_REFRESH",
                            remote_system=self.config.token_url,
                            address=self.config.token_url,
                            occurred_at=started_at,
                            completed_at=completed_at,
                            state=state,
                            attempt=attempt,
                            http_method="POST",
                            status_code=response.status_code,
                            request_payload=masked_payload,
                            request_content_type="application/x-www-form-urlencoded",
                            attrs={"error": str(err)},
                        )
                    )
                    # 4xx/5xx: do not retry — raise immediately
                    raise err

                # --- Success ---
                completed_at = datetime.now(UTC)
                state = "SUCCEEDED"
                token_data = response.json()
                self._access_token = token_data["access_token"]

                # Parse expiry
                expires_in = token_data.get("expires_in")
                if expires_in is not None:
                    try:
                        self._token_expires_at = datetime.now(UTC) + timedelta(
                            seconds=float(expires_in)
                        )
                    except (ValueError, TypeError):
                        self._token_expires_at = None
                else:
                    self._token_expires_at = None

                await record_data_exchange(
                    DataExchangeEvent(
                        integration=self.integration_name,
                        channel="HTTP",
                        operation="OAUTH2_TOKEN_REFRESH",
                        remote_system=self.config.token_url,
                        address=self.config.token_url,
                        occurred_at=started_at,
                        completed_at=completed_at,
                        state=state,
                        attempt=attempt,
                        http_method="POST",
                        status_code=response.status_code,
                        request_payload=masked_payload,
                        request_content_type="application/x-www-form-urlencoded",
                    )
                )
                return self._access_token

            except httpx.HTTPStatusError:
                raise
            except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as e:
                completed_at = datetime.now(UTC)
                state = "TIMEOUT" if isinstance(e, httpx.TimeoutException) else "FAILED"
                last_exc = e
                logger.warning(
                    f"OAuth2 token refresh transient error for {self.integration_name}"
                    f" (attempt {attempt}/{MAX_TOKEN_RETRIES}): {e}"
                )
                await record_data_exchange(
                    DataExchangeEvent(
                        integration=self.integration_name,
                        channel="HTTP",
                        operation="OAUTH2_TOKEN_REFRESH",
                        remote_system=self.config.token_url,
                        address=self.config.token_url,
                        occurred_at=started_at,
                        completed_at=completed_at,
                        state=state,
                        attempt=attempt,
                        http_method="POST",
                        request_payload=masked_payload,
                        request_content_type="application/x-www-form-urlencoded",
                        attrs={"error": str(e)},
                    )
                )
                if attempt >= MAX_TOKEN_RETRIES:
                    raise
                wait = 2**attempt
                await asyncio.sleep(wait)

        # Should not reach here, but satisfy type checker
        raise last_exc  # type: ignore[misc]

    def invalidate(self) -> None:
        """Clear the cached access token, forcing a refresh on the next request."""
        self._access_token = None
        self._token_expires_at = None


class HttpClient(BaseClient):
    """
    An enhanced, instrumented async HTTP client.
    Handles auth, retries, normalization, and detailed logging.
    """

    def __init__(self, name: str, settings: ClientSettings):
        self.name = name
        self.settings = settings

        # Normalization of base_url
        self.base_url = self._normalize_base_url(self.settings.base_url)
        if not self.base_url:
            raise ValueError("Base URL cannot be empty")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("Base URL must start with http:// or https://")

        self._clients_per_loop = weakref.WeakKeyDictionary()

        self._auth_manager = None
        if self.settings.auth:
            self._setup_auth(self.settings.auth)

    @property
    def client(self) -> httpx.AsyncClient:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = None

        if loop is None:
            return self._build_httpx_client()

        if loop not in self._clients_per_loop:
            self._clients_per_loop[loop] = self._build_httpx_client(use_base_url=False)

        return self._clients_per_loop[loop]

    def _build_httpx_client(self, use_base_url: bool = True) -> httpx.AsyncClient:
        # Extract extra configurations if present
        extra_headers = self.settings.extra.get("headers", {})
        extra_params = self.settings.extra.get("params", {})
        extra_cookies = self.settings.extra.get("cookies", {})

        # TLS / certificate resolution
        cert_param = None
        verify_param: Union[bool, str] = True
        if self.settings.tls:
            tls = self.settings.tls
            if tls.cert_file:
                cert_param = (
                    (tls.cert_file, tls.key_file) if tls.key_file else tls.cert_file
                )
            if tls.ca_bundle:
                verify_param = tls.ca_bundle
            elif not tls.verify_ssl:
                verify_param = False

        client = httpx.AsyncClient(
            base_url=self.base_url if use_base_url else "",
            timeout=self.settings.timeout,
            follow_redirects=self.settings.handle_redirects,
            headers=extra_headers,
            params=extra_params,
            cookies=extra_cookies,
            cert=cert_param,
            verify=verify_param,
        )
        if hasattr(self, "_basic_auth_tuple"):
            client.auth = httpx.BasicAuth(*self._basic_auth_tuple)
        return client

    def _normalize_base_url(self, url: str) -> str:
        if not url.endswith("/"):
            return url + "/"
        return url

    def _normalize_path(self, path: str) -> str:
        if path.startswith("/"):
            path = path.lstrip("/")
        return path

    def _match_path(self, pattern: str, path: str) -> bool:
        """
        Matches a path against a glob pattern.
        * matches a single path segment.
        ** matches recursively.
        """
        # Ensure path starts with / for consistent matching
        if not path.startswith("/"):
            path = "/" + path

        # Convert glob to regex
        # 1. Escape special regex characters
        # 2. ** -> RE_GLOB_RECURSIVE (temporary placeholder)
        # 3. * -> [^/]+ (matches one segment)
        # 4. RE_GLOB_RECURSIVE -> .* (matches anything)

        # Basic implementation using fnmatch for simpler cases
        # But fnmatch doesn't strictly adhere to ** for recursive directories in all versions
        # Let's do a more robust version:

        regex = fnmatch.translate(pattern)
        # fnmatch.translate adds \Z(?ms) at the end, and handles * as .*
        # We need to distinguish between * and ** if we want exact glob behavior

        # If pattern has **, it's recursive
        if "**" in pattern:
            # Replace ** with a placeholder that won't be escaped
            p = pattern.replace("**", "___RECURSIVE___")
            p = fnmatch.translate(p)
            p = p.replace("___RECURSIVE___", ".*")
            return bool(re.match(p, path))

        return fnmatch.fnmatch(path, pattern)

    async def _check_governance(
        self, method: str, path: str, url: str
    ) -> Optional[httpx.Response]:
        """
        Intercepts request based on Traffic Governance rules.
        Returns a mock response if behavior is MOCK, raises if RAISE, returns None if ALLOW.
        """
        if not self.settings.suppress:
            return None

        # Ensure path starts with / for matching
        match_path = path if path.startswith("/") else "/" + path

        # 1. Find the most specific match (longest prefix/pattern)
        best_rule = None
        best_match_len = -1

        for rule in self.settings.suppress:
            if self._match_path(rule.path, match_path):
                # Using length of pattern as a heuristic for "most specific"
                if len(rule.path) > best_match_len:
                    best_rule = rule
                    best_match_len = len(rule.path)

        if not best_rule:
            return None

        # 2. Method evaluation
        policy = best_rule.methods.get(method) or best_rule.methods.get("*")

        if not policy:
            return None

        # 3. Execution
        if policy.behaviour == Behaviour.ALLOW:
            return None

        if policy.behaviour == Behaviour.RAISE:
            raise SuppressedEndpointError(method, url)

        if policy.behaviour == Behaviour.MOCK:
            if not policy.mock_response:
                logger.warning(
                    f"Mock behavior defined for {method} {path} but no mock-response provided. Allowing."
                )
                return None

            mock = policy.mock_response
            content = None

            if mock.content:
                content = mock.content
            elif mock.file_ref:
                try:
                    # In a real scenario, we might want to resolve this relative to a base mocks dir
                    file_path = Path(mock.file_ref)
                    if not file_path.is_absolute():
                        # For now, let's assume relative to current working directory or predefined location
                        pass
                    if file_path.exists():
                        content = file_path.read_text()
                    else:
                        logger.error(f"Mock file not found: {mock.file_ref}")
                except Exception as e:
                    logger.error(f"Failed to read mock file {mock.file_ref}: {e}")

            return httpx.Response(
                status_code=mock.status,
                headers=mock.headers,
                content=content.encode("utf-8") if content else None,
                request=httpx.Request(method, url),
            )

        return None

    def _get_full_url(self, path: str) -> str:
        """
        Calculates the full URL for the request.
        If path is empty or ".", it uses the base_url without forced trailing slash.
        If path contains "://", it's treated as a full URL and returned as is.
        """
        if not path or path == ".":
            return self.settings.base_url

        if "://" in path:
            return path

        # httpx-style join: Ensure base ends with / and path doesn't start with /
        base = self.base_url
        p = self._normalize_path(path)
        return f"{base}{p}"

    def _setup_auth(self, auth_config: AuthConfig):
        if auth_config.type == AuthType.BASIC:
            self._basic_auth_tuple = (auth_config.username, auth_config.password)
        elif auth_config.type == AuthType.API_KEY:
            # Handled in request()
            pass
        elif auth_config.type == AuthType.OAUTH2:
            self._auth_manager = OAuth2Manager(auth_config, integration_name=self.name)

    def _mask_url_auth_params(self, url: str, auth_param_keys: set) -> str:
        """Return url with values of auth-injected and known-sensitive query params masked."""
        mask_keys = {k.lower() for k in auth_param_keys} | _SENSITIVE_PARAM_KEYS
        try:
            parsed = urlparse(url)
            if not parsed.query:
                return url
            parts = []
            for pair in parsed.query.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    if any(s in k.lower() for s in mask_keys):
                        parts.append(f"{k}={_mask_value(v)}")
                    else:
                        parts.append(pair)
                else:
                    parts.append(pair)
            return urlunparse(parsed._replace(query="&".join(parts)))
        except Exception:
            return url

    def mask_sensitive_data(self, event: DataExchangeEvent) -> DataExchangeEvent:
        import dataclasses

        def _mask_headers(headers):
            if not headers:
                return headers
            return {
                k: v if k.lower() in _UNMASKED_HEADER_KEYS else _mask_value(v)
                for k, v in headers.items()
            }

        return dataclasses.replace(
            event,
            request_headers=_mask_headers(event.request_headers),
            response_headers=_mask_headers(event.response_headers),
        )

    async def authorize(
        self, headers: Dict[str, str], params: Dict[str, Any], cookies: Dict[str, str]
    ) -> set:
        """Apply auth in-place. Returns set of query-param keys injected (for masking)."""
        if not self.settings.auth:
            return set()

        injected_param_keys: set = set()
        auth = self.settings.auth
        if auth.type == AuthType.API_KEY:
            if auth.in_ == ApiKeyLocation.HEADER:
                headers[auth.key] = auth.value
            elif auth.in_ == ApiKeyLocation.QUERY:
                params[auth.key] = auth.value
                injected_param_keys.add(auth.key)
        elif auth.type == AuthType.OAUTH2 and self._auth_manager:
            token = await self._auth_manager.get_token(self.client)
            headers["Authorization"] = f"Bearer {token}"
        elif auth.type == AuthType.BASIC:
            pass  # Handled by client.auth
        else:
            raise ValueError(f"Unsupported auth type: {auth.type}")
        return injected_param_keys

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        content: Optional[Any] = None,
        files: Optional[Any] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> httpx.Response:
        url = self._get_full_url(path)
        headers = headers or {}
        params = params or {}
        cookies = cookies or {}

        # Traffic Governance: Suppression Check
        governance_response = await self._check_governance(method, path, url)
        if governance_response:
            # If governance returned a response, it's a mock
            logger.info(f"Request to {method} {url} intercepted by mock governance.")
            return governance_response

        # Additional Auth Handling
        try:
            injected_param_keys = await self.authorize(headers, params, cookies)
        except Exception as auth_exc:
            logger.error(
                f"Authorization failed for {self.name} [{method} {url}]: {auth_exc}"
            )
            raise

        # Snapshot request headers after auth injection; build masked address
        req_headers_snapshot = dict(headers)
        masked_address = self._mask_url_auth_params(url, injected_param_keys)

        # Observability: Extract Request Payload
        req_content_type = headers.get("Content-Type")
        req_body_bytes = None
        try:
            if json is not None:
                import json as json_lib

                req_body_bytes = json_lib.dumps(json).encode("utf-8")
                if not req_content_type:
                    req_content_type = "application/json"
            elif content is not None and isinstance(content, (bytes, str)):
                req_body_bytes = (
                    content.encode("utf-8") if isinstance(content, str) else content
                )
            elif data is not None and isinstance(data, (bytes, str)):
                req_body_bytes = data.encode("utf-8") if isinstance(data, str) else data
        except Exception as e:
            logger.warning(f"Failed to extract request payload for observability: {e}")

        start_time = datetime.now(UTC)

        # Retry Loop
        max_retries = self.settings.retry.max_retries
        current_try = 0

        while True:
            response = None
            try:
                response = await self.client.request(
                    method,
                    url,
                    params=params or None,
                    headers=headers,
                    json=json,
                    data=data,
                    content=content,
                    files=files,
                    cookies=cookies,
                    timeout=timeout,
                )

                if response.is_error:
                    wait = await self._should_retry(response, current_try, max_retries)
                    if wait is not None:
                        current_try += 1
                        logger.info(
                            f"Retrying request to {self.name} ({current_try}/{max_retries}) after {wait:.1f}s..."
                        )
                        await asyncio.sleep(wait)
                        # Re-authorize in case token was just invalidated (e.g. 401 + OAuth2)
                        try:
                            await self.authorize(headers, params, cookies)
                        except Exception as re_auth_exc:
                            logger.error(
                                f"Re-authorization failed for {self.name} after 401"
                                f" [{method} {url}]: {re_auth_exc}"
                            )
                            raise
                        continue

                    # Generate curl command for debugging
                    curl_cmd = self._to_curl_command(
                        method,
                        url,
                        headers,
                        params,
                        json_payload=json,
                        data_payload=data,
                        content_payload=content,
                    )
                    await self._log_error_response(response, curl_cmd)
                    # Don't raise yet, handle Observability FAILED below

                # Observability: Handle Response
                resp_content_type = response.headers.get("Content-Type")
                resp_bytes = None
                try:
                    resp_bytes = await response.aread()
                except Exception as e:
                    logger.warning(f"Failed to read response payload: {e}")

                end_time = datetime.now(UTC)
                state = "SUCCEEDED" if not response.is_error else "FAILED"

                await self._emit_data_exchange(
                    DataExchangeEvent(
                        integration=self.settings.client_id,
                        channel="HTTP",
                        operation=f"{method} {path}",
                        remote_system=self.base_url,
                        address=masked_address,
                        occurred_at=start_time,
                        completed_at=end_time,
                        state=state,
                        attempt=current_try + 1,
                        http_method=method,
                        status_code=response.status_code,
                        response_payload=resp_bytes,
                        response_content_type=resp_content_type,
                        request_payload=req_body_bytes,
                        request_content_type=req_content_type,
                        request_headers=req_headers_snapshot,
                        response_headers=dict(response.headers),
                        offload_payloads=files is not None,
                    ),
                    correlation=None,
                )

                if response.is_error:
                    body = response.text
                    message = (
                        f"HTTP Error {response.status_code} for {method} {path} "
                        f"({url})\n"
                        f"Response: {body}"
                    )
                    raise httpx.HTTPStatusError(
                        message, request=response.request, response=response
                    )

                return response

            except httpx.HTTPStatusError:
                raise
            except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as e:
                # Generate curl command for debugging
                curl_cmd = self._to_curl_command(
                    method,
                    url,
                    headers,
                    params,
                    json_payload=json,
                    data_payload=data,
                    content_payload=content,
                )
                logger.error(
                    f"HTTP Connection/Timeout Error: {e}\nReplicate with:\n{curl_cmd}"
                )

                if current_try < max_retries:
                    current_try += 1
                    wait = self._compute_backoff(current_try)
                    logger.warning(
                        f"Connection error to {self.name}, retrying ({current_try}/{max_retries}) in {wait:.1f}s: {e}"
                    )
                    await asyncio.sleep(wait)
                    continue

                # FINAL FAILURE - Observability: TIMEOUT/FAILED
                end_time = datetime.now(UTC)

                await self._emit_data_exchange(
                    DataExchangeEvent(
                        integration=self.settings.client_id,
                        channel="HTTP",
                        operation=f"{method} {path}",
                        remote_system=self.base_url,
                        address=masked_address,
                        occurred_at=start_time,
                        completed_at=end_time,
                        state=(
                            "TIMEOUT"
                            if isinstance(e, httpx.TimeoutException)
                            else "FAILED"
                        ),
                        attempt=current_try + 1,
                        http_method=method,
                        attrs={"error": str(e)},
                        request_payload=req_body_bytes,
                        request_content_type=req_content_type,
                        request_headers=req_headers_snapshot,
                    ),
                    correlation=None,
                )

                raise e

    def _compute_backoff(self, current_try: int) -> float:
        """Exponential backoff with jitter, capped at max_wait."""
        import random

        raw = 2 ** (current_try + 1)
        return min(raw + random.uniform(0, 1), self.settings.retry.max_wait)

    async def _should_retry(
        self,
        response: httpx.Response,
        current_try: int,
        max_retries: int,
    ) -> Optional[float]:
        """Return seconds to wait before retry, or None to not retry."""
        if current_try >= max_retries:
            return None

        # 401 with OAuth2: invalidate the cached token so the next attempt
        # fetches a fresh one, then allow the retry.
        if response.status_code == 401 and self._auth_manager is not None:
            self._auth_manager.invalidate()
            return self._compute_backoff(current_try)

        if response.status_code in [429, 500, 502, 503, 504]:
            retry_possible = True
        else:
            retry_possible = False

        body = response.text
        for pattern in self.settings.retry.blacklist:
            if pattern in body:
                logger.debug(
                    f"Retry blacklisted due to pattern '{pattern}' in response."
                )
                return None

        for pattern in self.settings.retry.whitelist:
            if pattern in body:
                logger.debug(
                    f"Retry whitelisted due to pattern '{pattern}' in response."
                )
                return self._compute_backoff(current_try)

        if not retry_possible:
            return None

        # Respect Retry-After header when present (common on 429)
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait = min(float(retry_after), self.settings.retry.max_wait)
                return wait
            except ValueError:
                pass

        return self._compute_backoff(current_try)

    async def _log_error_response(
        self, response: httpx.Response, curl_cmd: Optional[str] = None
    ):
        try:
            body = response.text
            log_msg = (
                f"HTTP Error {response.status_code} from {self.name}\n"
                f"URL: {response.url}\n"
                f"Response Body: {body[:1000]}{'...' if len(body) > 1000 else ''}"
            )
            if curl_cmd:
                log_msg += f"\nReplicate with:\n{curl_cmd}"

            logger.error(log_msg)
        except Exception as e:
            logger.error(f"Failed to log error response: {e}")

    async def close(self):
        for client in self._clients_per_loop.values():
            await client.aclose()
        self._clients_per_loop.clear()

    def _to_curl_command(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        json_payload: Optional[Any] = None,
        data_payload: Optional[Any] = None,
        content_payload: Optional[Any] = None,
    ) -> str:
        """
        Generates a curl command to replicate the request.
        Masks common secrets like 'Authorization' and 'x-api-key'.
        """
        try:
            import shlex
            from urllib.parse import urlencode

            def _mask_secret(val: str) -> str:
                if not val or len(val) <= 4:
                    return "***"
                return f"***{val[-4:]}"

            cmd_parts = ["curl", "-v", "-X", method]

            # Reconstruct URL with query params
            full_url = url
            if params:
                query_string = urlencode(params)
                connector = "&" if "?" in full_url else "?"
                full_url = f"{full_url}{connector}{query_string}"

            cmd_parts.append(shlex.quote(full_url))

            # Sensitive header keys to mask
            SENSITIVE_HEADERS = {
                "authorization",
                "x-api-key",
                "cookie",
                "proxy-authorization",
            }

            # Add Headers
            for k, v in headers.items():
                if k.lower() in SENSITIVE_HEADERS:
                    v = _mask_secret(v)
                cmd_parts.extend(["-H", shlex.quote(f"{k}: {v}")])

            # Add client certificate flags for mTLS
            if self.settings.tls and self.settings.tls.cert_file:
                cmd_parts.extend(["--cert", shlex.quote(self.settings.tls.cert_file)])
                if self.settings.tls.key_file:
                    cmd_parts.extend(["--key", shlex.quote(self.settings.tls.key_file)])

            # Add Body
            if json_payload is not None:
                import json as json_lib

                try:
                    # Deep copy if possible to mask nested secrets
                    import copy

                    masked_json = copy.deepcopy(json_payload)

                    def mask_json_secrets(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                if any(
                                    s in k.lower()
                                    for s in ["secret", "password", "key", "token"]
                                ):
                                    if isinstance(v, str):
                                        obj[k] = _mask_secret(v)
                                mask_json_secrets(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                mask_json_secrets(item)

                    mask_json_secrets(masked_json)
                    body_str = json_lib.dumps(masked_json)
                    cmd_parts.extend(["--data-raw", shlex.quote(body_str)])
                except Exception:
                    cmd_parts.append("--data-raw '<failed to serialize json>'")
            elif data_payload is not None:
                if isinstance(data_payload, dict):
                    # Mask common secret fields in form data
                    masked_data = data_payload.copy()
                    for k in masked_data:
                        if any(
                            s in k.lower()
                            for s in ["secret", "password", "key", "token"]
                        ):
                            masked_data[k] = _mask_secret(str(masked_data[k]))
                    body_str = urlencode(masked_data)
                    cmd_parts.extend(["-d", shlex.quote(body_str)])
                else:
                    cmd_parts.extend(["--data-raw", shlex.quote(str(data_payload))])
            elif content_payload is not None:
                if isinstance(content_payload, bytes):
                    cmd_parts.extend(
                        [
                            "--data-raw",
                            shlex.quote(
                                content_payload.decode("utf-8", errors="replace")
                            ),
                        ]
                    )
                else:
                    cmd_parts.extend(["--data-raw", shlex.quote(str(content_payload))])

            return " ".join(cmd_parts)
        except Exception as e:
            return f"<Error generating curl command: {e}>"

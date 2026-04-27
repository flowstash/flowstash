import httpx
import logging
import asyncio
import datetime
import fnmatch
import re
from pathlib import Path
from datetime import UTC
from typing import Any, Optional, Dict, List, Union
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


class SuppressedEndpointError(httpx.HTTPError):
    """Raised when a request is suppressed by traffic governance rules."""

    def __init__(self, method: str, url: str):
        super().__init__(f"Request suppressed by governance: {method} {url}")


class OAuth2Manager:
    """
    Primitive OAuth2 manager for token handling and re-auth.
    """

    def __init__(self, config: OAuth2AuthConfig):
        self.config = config
        self._access_token = None
        self._lock = asyncio.Lock()

    async def get_token(self, client: httpx.AsyncClient) -> str:
        async with self._lock:
            if self._access_token:
                return self._access_token
            return await self.refresh_token(client)

    async def refresh_token(self, client: httpx.AsyncClient) -> str:
        logger.info(
            f"Refreshing OAuth2 token for {self.config.client_id} via {self.config.token_url}"
        )

        # Determine grant_type
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
            # client_credentials doesn't need extra fields usually
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

        response = await client.post(self.config.token_url, data=data, **post_kwargs)
        if response.is_error:
            body = response.text

            # Generate curl command for debugging a failed token refresh
            curl_cmd = "<Error generating curl command>"
            try:
                import shlex
                from urllib.parse import urlencode

                def _mask_secret(val: str) -> str:
                    if not val or len(val) <= 4:
                        return "***"
                    return f"***{val[-4:]}"

                cmd_parts = [
                    "curl",
                    "-v",
                    "-X",
                    "POST",
                    shlex.quote(self.config.token_url),
                ]
                # Mask secrets in data
                masked_data = data.copy()
                if "client_secret" in masked_data:
                    masked_data["client_secret"] = _mask_secret(
                        masked_data["client_secret"]
                    )
                if "password" in masked_data:
                    masked_data["password"] = _mask_secret(masked_data["password"])

                body_str = urlencode(masked_data)
                cmd_parts.extend(["-d", shlex.quote(body_str)])
                curl_cmd = " ".join(cmd_parts)
            except Exception as e:
                logger.warning(
                    f"Failed to generate curl command for token refresh error: {e}"
                )

            raise httpx.HTTPStatusError(
                f"OAuth2 token refresh failed with status {response.status_code}. Response: {body}\nReplicate with:\n{curl_cmd}",
                request=response.request,
                response=response,
            )
        self._access_token = response.json()["access_token"]
        return self._access_token

    def invalidate(self) -> None:
        """Clear the cached access token, forcing a refresh on the next request."""
        self._access_token = None


class HttpClient:
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
        """
        if not path or path == ".":
            return self.settings.base_url

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
            self._auth_manager = OAuth2Manager(auth_config)

    async def authorize(
        self, headers: Dict[str, str], params: Dict[str, Any], cookies: Dict[str, str]
    ) -> None:
        if not self.settings.auth:
            return

        auth = self.settings.auth
        if auth.type == AuthType.API_KEY:
            if auth.in_ == ApiKeyLocation.HEADER:
                headers[auth.key] = auth.value
            elif auth.in_ == ApiKeyLocation.QUERY:
                params[auth.key] = auth.value
        elif auth.type == AuthType.OAUTH2 and self._auth_manager:
            token = await self._auth_manager.get_token(self.client)
            headers["Authorization"] = f"Bearer {token}"
        elif auth.type == AuthType.BASIC:
            pass  # Handled by client.auth
        else:
            raise ValueError(f"Unsupported auth type: {auth.type}")

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
            # For observability, we still want to record this
            start_time = datetime.datetime.now(UTC)
            await record_data_exchange(
                DataExchangeEvent(
                    integration=self.settings.client_id,
                    channel="HTTP (Mock)",
                    operation=f"{method} {path}",
                    remote_system=self.base_url,
                    address=url,
                    occurred_at=start_time,
                    completed_at=start_time,
                    state="SUCCEEDED",
                    attempt=1,
                    http_method=method,
                    status_code=governance_response.status_code,
                    response_payload=governance_response.content,
                    response_content_type=governance_response.headers.get(
                        "Content-Type"
                    ),
                ),
                correlation=None,
            )
            return governance_response

        # Additional Auth Handling
        await self.authorize(headers, params, cookies)

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

        start_time = datetime.datetime.now(UTC)

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
                        await self.authorize(headers, params, cookies)
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

                end_time = datetime.datetime.now(UTC)
                state = "SUCCEEDED" if not response.is_error else "FAILED"

                await record_data_exchange(
                    DataExchangeEvent(
                        integration=self.settings.client_id,
                        channel="HTTP",
                        operation=f"{method} {path}",
                        remote_system=self.base_url,
                        address=url,
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
            except (httpx.ConnectError, httpx.TimeoutException) as e:
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
                end_time = datetime.datetime.now(UTC)

                await record_data_exchange(
                    DataExchangeEvent(
                        integration=self.settings.client_id,
                        channel="HTTP",
                        operation=f"{method} {path}",
                        remote_system=self.base_url,
                        address=url,
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

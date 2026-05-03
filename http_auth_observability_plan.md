# Plan: HTTP Auth Observability, Token Expiry & Retry

## Problem Summary

Four issues exist in `packages/flowstash_clients/src/flowstash/clients/http.py`:

1. **Auth failure is invisible** — if `authorize()` raises (e.g. OAuth2 token refresh fails), `request()` propagates the exception without logging or calling `record_data_exchange`.
2. **OAuth2 token expiry not tracked** — `OAuth2Manager` caches the token but never stores `expires_in`. It only re-fetches after a 401 hit, causing one avoidable failed request per token cycle.
3. **No retry on auth/token-refresh failure** — transient network errors on the token endpoint are not retried.
4. **Sensitive values in observability URL** — when API key auth uses `in=query`, the full URL (with the key value) is passed as `address` to `record_data_exchange`.

---

## Scope

All changes are confined to a single file:

```
packages/flowstash_clients/src/flowstash/clients/http.py
```

Tests that need new cases:

```
packages/flowstash_lib/tests/test_http.py
packages/flowstash_lib/tests/test_http_auth.py
```

---

## Approach Comparison

There are two meaningful choices for where token-refresh observability lives.

### Approach A — Record inside `OAuth2Manager.refresh_token()`

The token exchange HTTP call is recorded within the OAuth2Manager, with full timing and masked credentials. `HttpClient` only needs to catch and log the resulting exception.

**Pros:**
- Correct address and timing for the token endpoint are captured naturally.
- Auth observability is co-located with auth logic.
- `HttpClient.request()` remains clean.

**Cons:**
- `OAuth2Manager` gains a dependency on `record_data_exchange` (already imported at module level in `http.py`, so not a new import).

### Approach B — Record in `HttpClient.request()` after `authorize()` failure

`HttpClient.request()` wraps `authorize()` in try/except and emits the data-exchange event there.

**Pros:**
- `OAuth2Manager` stays simpler.

**Cons:**
- The `address` field would be the original request URL, not the token endpoint. Misleading — the failed call was to the token server.
- Timing starts from the outer `start_time`, not from when the token call began.

### Recommendation

**Approach A.** Record the token refresh call inside `OAuth2Manager.refresh_token()`, and only add a brief `logger.error` in `HttpClient.request()` on auth exception.

---

## Detailed Implementation

### 1. Add `timedelta` to imports (top of file)

```python
# existing:
from datetime import UTC
# change to:
from datetime import UTC, timedelta
```

---

### 2. `OAuth2Manager` — token expiry fields

Add two instance fields to `__init__`:

```python
self._token_expires_at: Optional[datetime] = None
self._expiry_buffer_seconds: int = 30
```

`_token_expires_at` is the `datetime` (UTC) when the current token expires.  
`_expiry_buffer_seconds` is how many seconds before actual expiry we treat the token as stale (pre-emptive refresh).

---

### 3. `OAuth2Manager._is_token_valid()` — new private method

```python
def _is_token_valid(self) -> bool:
    if not self._access_token:
        return False
    if self._token_expires_at is None:
        return True          # no expiry info from server, assume valid until 401
    return datetime.now(UTC) < (self._token_expires_at - timedelta(seconds=self._expiry_buffer_seconds))
```

---

### 4. `OAuth2Manager.get_token()` — use `_is_token_valid()`

Replace:
```python
if self._access_token:
    return self._access_token
```
With:
```python
if self._is_token_valid():
    return self._access_token
```

---

### 5. `OAuth2Manager.invalidate()` — also clear expiry

```python
def invalidate(self) -> None:
    self._access_token = None
    self._token_expires_at = None
```

---

### 6. `OAuth2Manager.refresh_token()` — retry + expiry parsing + observability

This is the largest change. The method gains:

- **Retry loop** (max 3 attempts) for transient network/timeout errors only; 4xx errors are not retried.
- **Expiry parsing** — read `expires_in` from the token response and set `_token_expires_at`.
- **Observability** — call `record_data_exchange` for each attempt, with masked request payload. Failed attempts are logged.

#### Masking helper (module-level or inner function)

A module-level private function `_mask_oauth_payload(data: dict) -> bytes` that:
- accepts the form-data dict sent to the token endpoint
- replaces values of keys matching `client_secret`, `password`, `refresh_token` with `***{last4}`
- URL-encodes and returns as `bytes`

Signature:
```python
def _mask_oauth_payload(data: dict) -> bytes: ...
```

#### Retry loop in `refresh_token()`

```
for attempt in range(1, MAX_TOKEN_RETRIES + 1):
    started_at = datetime.now(UTC)
    try:
        response = await client.post(token_url, data=data, **post_kwargs)
        completed_at = datetime.now(UTC)
        if response.is_error:
            # record FAILED, log error, raise HTTPStatusError (no retry on 4xx)
            ...
        # parse expires_in, set _token_expires_at
        # record SUCCEEDED
        self._access_token = response.json()["access_token"]
        return self._access_token
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        completed_at = datetime.now(UTC)
        state = "TIMEOUT" if isinstance(e, httpx.TimeoutException) else "FAILED"
        # record FAILED with state
        # log warning
        if attempt >= MAX_TOKEN_RETRIES:
            raise
        wait = 2 ** attempt  # simple backoff
        await asyncio.sleep(wait)
```

`MAX_TOKEN_RETRIES = 3` — module-level constant.

#### `record_data_exchange` call for token refresh

```python
await record_data_exchange(
    DataExchangeEvent(
        integration=self.config.client_id,   # if exposed; or use token_url host
        channel="HTTP",
        operation="OAUTH2_TOKEN_REFRESH",
        remote_system=self.config.token_url,
        address=self.config.token_url,
        occurred_at=started_at,
        completed_at=completed_at,
        state=state,          # "SUCCEEDED" / "FAILED" / "TIMEOUT"
        attempt=attempt,
        http_method="POST",
        status_code=response.status_code if response else None,
        request_payload=_mask_oauth_payload(data),
        request_content_type="application/x-www-form-urlencoded",
        attrs={"error": str(exc)} if failed else {},
    )
)
```

Note: `OAuth2AuthConfig` already has `client_id` field but no `integration` name. Pass `self.config.client_id` for `integration`. Alternatively expose a `name` parameter from `HttpClient` when constructing `OAuth2Manager` — see option below.

**Option: pass `integration_name` into `OAuth2Manager`**

When `HttpClient._setup_auth()` creates the manager, pass `self.name`:

```python
self._auth_manager = OAuth2Manager(auth_config, integration_name=self.name)
```

And `OAuth2Manager.__init__` gains:

```python
self.integration_name: str = integration_name or config.client_id
```

This is the recommended approach so the data-exchange event carries the same integration name as the main request events.

---

### 7. `HttpClient.request()` — catch auth failure + log

The pre-loop `await self.authorize(headers, params, cookies)` is already before the retry loop. Wrap it:

```python
try:
    await self.authorize(headers, params, cookies)
except Exception as auth_exc:
    logger.error(
        f"Authorization failed for {self.name} [{method} {url}]: {auth_exc}"
    )
    raise
```

We do **not** emit a second `record_data_exchange` here because the OAuth2 path already recorded the token endpoint call. For future non-OAuth2 auth types that could fail (e.g. CUSTOM), this log is sufficient; if needed a record can be added later.

The re-authorize call inside the retry loop (after 401 invalidate) should get the same treatment:

```python
try:
    await self.authorize(headers, params, cookies)
except Exception as re_auth_exc:
    logger.error(
        f"Re-authorization failed for {self.name} after 401 [{method} {url}]: {re_auth_exc}"
    )
    raise
```

---

### 8. `HttpClient._mask_sensitive_url()` — new private method

For API-key-in-query, the full URL including the secret is passed to `record_data_exchange`. Add:

```python
def _mask_sensitive_url(self, url: str) -> str:
    """Return URL with values of sensitive query params replaced by ***."""
    ...
```

Implementation:
- Use `urllib.parse.urlparse` + `urllib.parse.parse_qs` / `urlencode`
- Mask values of params where the key matches any of:  
  `api_key`, `apikey`, `key`, `token`, `secret`, `password`, `access_token`, `client_secret`  
  (case-insensitive substring match)
- Return reconstructed URL

Use this in `request()` when building `DataExchangeEvent`:

```python
address=self._mask_sensitive_url(url),
```

Both the success path and the connection-error path need updating.

---

## Flow Diagram (After Changes)

```
HttpClient.request(method, path, ...)
│
├─ governance check (unchanged)
│
├─ try: await self.authorize(headers, params, cookies)
│    └─ OAuth2Manager.get_token()
│         ├─ _is_token_valid() → True  → return cached token (no HTTP call)
│         └─ _is_token_valid() → False → refresh_token()
│              ├─ retry loop (max 3)
│              │    ├─ POST token_url  (masked payload)
│              │    ├─ record_data_exchange (OAUTH2_TOKEN_REFRESH, SUCCEEDED/FAILED/TIMEOUT)
│              │    └─ on transient error: sleep + retry
│              └─ parse expires_in → set _token_expires_at
│   except Exception:
│       logger.error(...)
│       raise
│
└─ retry loop (unchanged structure)
     ├─ client.request(...)
     ├─ if 401 + OAuth2: invalidate() + re-authorize (with try/except + logger.error)
     ├─ record_data_exchange(address=_mask_sensitive_url(url), ...)
     └─ raise or return
```

---

## Test Cases to Add

### `test_http_auth.py`

| Test | Description |
|------|-------------|
| `test_oauth2_token_expiry_refresh` | Token is cached; after `_token_expires_at` passes, `refresh_token` is called again |
| `test_oauth2_token_not_refreshed_if_valid` | Token is valid and not near expiry; `client.post` is called only once |
| `test_oauth2_refresh_retry_on_connect_error` | `ConnectError` on first two attempts; succeeds on third; records 3 data-exchange events |
| `test_oauth2_refresh_no_retry_on_401` | Token endpoint returns 401; raises immediately without retry |
| `test_oauth2_refresh_records_data_exchange` | Happy path; verifies `record_data_exchange` called with `operation="OAUTH2_TOKEN_REFRESH"` and masked payload |
| `test_oauth2_refresh_failure_logs_error` | Token endpoint fails; verifies `logger.error` was called |
| `test_authorize_failure_logs_error` | OAuth2 refresh permanently fails; `HttpClient.request()` logs error before propagating |
| `test_api_key_query_url_masked_in_record` | API key in query; `record_data_exchange` address does NOT contain the secret value |

### `test_http.py`

| Test | Description |
|------|-------------|
| `test_masked_url_strips_api_key_query_param` | Unit test for `_mask_sensitive_url()` |

---

## Summary of New Symbols

| Symbol | Kind | Location | Notes |
|--------|------|----------|-------|
| `_mask_oauth_payload(data: dict) -> bytes` | module function | `http.py` | masks credentials before recording |
| `OAuth2Manager._is_token_valid() -> bool` | method | `OAuth2Manager` | checks cached token against expiry |
| `OAuth2Manager._token_expires_at: Optional[datetime]` | field | `OAuth2Manager` | set from `expires_in` in token response |
| `OAuth2Manager._expiry_buffer_seconds: int` | field | `OAuth2Manager` | default 30s |
| `OAuth2Manager.integration_name: str` | field | `OAuth2Manager` | passed from `HttpClient._setup_auth()` |
| `MAX_TOKEN_RETRIES: int = 3` | module constant | `http.py` | retries for transient token endpoint failures |
| `HttpClient._mask_sensitive_url(url: str) -> str` | method | `HttpClient` | strips secret values from query params |

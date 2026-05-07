# Plan: DataExchangeEvent Headers, Params Masking & BaseClient Refactor

## Summary of Changes

Three related problems to solve together:

1. **Add `request_headers` / `response_headers` fields** to `DataExchangeEvent` and `DataExchange` (model + ingestion wiring)
2. **Mask header values** (except `content-type`) using `first3...last3` format; **mask only auth-injected query-param values** (not all params) using the same format — masking happens in `authorize()` by returning the set of param keys it injected, which `mask_sensitive_data` then uses to selectively mask in the recorded `address`
3. **Move `record_data_exchange` emission into `BaseClient`** with an overridable `mask_sensitive_data` hook, so `HttpClient` customises masking and all call sites in `HttpClient.request()` go through one path

---

## Files Affected

| File | Change |
|---|---|
| `packages/flowstash_lib/src/flowstash/observability/model.py` | Add headers fields to `DataExchangeEvent` and `DataExchange` |
| `packages/flowstash_lib/src/flowstash/observability/ingestion.py` | Wire headers through in `record_data_exchange` → `DataExchange` construction |
| `packages/flowstash_clients/src/flowstash/clients/base.py` | Add `_emit_data_exchange` + `mask_sensitive_data` methods |
| `packages/flowstash_clients/src/flowstash/clients/http.py` | Collect headers, override `mask_sensitive_data`, replace call sites |

---

## Detailed Changes

### 1. `model.py` — Add headers to `DataExchangeEvent` and `DataExchange`

#### `DataExchangeEvent` dataclass
Add two new optional fields (after `response_size_bytes`):

```python
request_headers: Optional[Dict[str, str]] = None
response_headers: Optional[Dict[str, str]] = None
```

The type annotation `Dict` already requires `from typing import Dict` (already present).

#### `DataExchange` dataclass
Add two new optional fields (after `response_size_bytes`):

```python
request_headers: Optional[Dict[str, str]] = None
response_headers: Optional[Dict[str, str]] = None
```

Both dataclasses are `frozen=True`; new fields with defaults are backward-compatible.

---

### 2. `ingestion.py` — Wire headers through to `DataExchange`

In `record_data_exchange` → `_process_dx` inner function, update the `DataExchange(...)` constructor call to add:

```python
request_headers=event.request_headers,
response_headers=event.response_headers,
```

No other logic change needed here.

---

### 3. `base.py` — New `_emit_data_exchange` and `mask_sensitive_data` methods

`BaseClient` currently has only one classmethod (`get_client`). We add two instance methods:

```python
from flowstash.observability.model import DataExchangeEvent
from flowstash.observability.ingestion import record_data_exchange
from typing import Optional
from flowstash.observability.model import Correlation

class BaseClient:

    @classmethod
    def get_client(cls: Type[T]) -> T:
        ...  # unchanged

    def mask_sensitive_data(self, event: "DataExchangeEvent") -> "DataExchangeEvent":
        """
        Override in subclasses to redact secrets from the event before emission.
        Default implementation is a pass-through (no masking).
        """
        return event

    async def _emit_data_exchange(
        self,
        event: "DataExchangeEvent",
        correlation: "Optional[Correlation]" = None,
    ) -> None:
        """
        Mask then emit a DataExchangeEvent.
        All subclass call sites should use this instead of calling
        record_data_exchange directly.
        """
        masked = self.mask_sensitive_data(event)
        await record_data_exchange(masked, correlation)
```

**Why a method on BaseClient instead of a standalone helper?**
- Subclasses of `BaseClient` (e.g. a future `FtpClient`, `S3Client`) inherit the same emission pipeline.
- Masking is state-aware (the client knows its own auth type, configured sensitive keys, etc.).
- Subclasses that don't need custom masking get correct behaviour for free.

---

### 4. `http.py` — Override `mask_sensitive_data`, collect headers, update call sites

#### 4a. New module-level masking helper

Introduce a shared value-masking function:

```python
def _mask_value(val: str) -> str:
    """Mask a value: show first 3 and last 3 chars, hide the rest.
    Falls back to *** for short values (≤6 chars).
    """
    if not val or len(val) <= 6:
        return "***"
    return f"{val[:3]}...{val[-3:]}"
```

`_mask_value` is used for both header masking and the auth-param URL masking.

#### 4b. Update `_mask_sensitive_url` to use `_mask_value` format (keep selective masking)

The existing `_mask_sensitive_url` already masks only keys in `_SENSITIVE_PARAM_KEYS`. The only change is replacing the `***` placeholder with `_mask_value(v)`. The function stays as-is semantically — it masks only known auth/sensitive param names, **not** all params:

```python
def _mask_sensitive_url(self, url: str) -> str:
    """Return url with values of sensitive query params masked."""
    try:
        parsed = urlparse(url)
        if not parsed.query:
            return url
        parts = []
        for pair in parsed.query.split("&"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if any(s in k.lower() for s in _SENSITIVE_PARAM_KEYS):
                    parts.append(f"{k}={_mask_value(v)}")
                else:
                    parts.append(pair)
            else:
                parts.append(pair)
        return urlunparse(parsed._replace(query="&".join(parts)))
    except Exception:
        return url
```

`_SENSITIVE_PARAM_KEYS` is kept unchanged and also continues to serve `_to_curl_command`.

However, `_mask_sensitive_url` alone misses API key params whose key names don't happen to match `_SENSITIVE_PARAM_KEYS` (e.g. a custom key name configured by the user). To solve this, `authorize()` returns the set of param keys it injected, and those are always masked.

#### 4c. Change `authorize()` signature to return injected param keys

```python
async def authorize(
    self, headers: Dict[str, str], params: Dict[str, Any], cookies: Dict[str, str]
) -> set[str]:
    """
    Apply auth to headers/params/cookies in-place.
    Returns the set of query-param keys injected (so callers can mask them in the address).
    """
    injected_param_keys: set[str] = set()
    ...
    if auth.type == AuthType.API_KEY:
        if auth.in_ == ApiKeyLocation.HEADER:
            headers[auth.key] = auth.value
        elif auth.in_ == ApiKeyLocation.QUERY:
            params[auth.key] = auth.value
            injected_param_keys.add(auth.key)
    ...
    return injected_param_keys
```

The caller in `request()` captures the result:

```python
injected_param_keys = await self.authorize(headers, params, cookies)
```

#### 4d. New helper `_mask_url_auth_params(url, keys)`

```python
def _mask_url_auth_params(self, url: str, auth_param_keys: set[str]) -> str:
    """Mask values of specific query-param keys (auth-injected) plus _SENSITIVE_PARAM_KEYS."""
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
```

This replaces all usages of `self._mask_sensitive_url(url)` in `request()`. The old `_mask_sensitive_url` method can be removed; `_to_curl_command` can use `_mask_url_auth_params` or keep its own logic.

#### 4e. Override `mask_sensitive_data` in `HttpClient`

```python
# Headers whose values are NOT masked
_UNMASKED_HEADER_KEYS = frozenset(["content-type"])

def mask_sensitive_data(self, event: DataExchangeEvent) -> DataExchangeEvent:
    import dataclasses

    def _mask_headers(headers: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
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
        # address is already masked at construction time via _mask_url_auth_params
    )
```

Note: the `address` is already correct before `mask_sensitive_data` is called, because `_mask_url_auth_params` is called in `request()` at the point where `injected_param_keys` is known (right after `authorize()`). `mask_sensitive_data` handles only header masking.

#### 4f. Collect headers in `HttpClient.request()` and pass to event

After `authorize()` runs, snapshot request headers. After response arrives, snapshot response headers:

```python
injected_param_keys = await self.authorize(headers, params, cookies)
req_headers_snapshot = dict(headers)   # includes auth-injected headers

# ... after response:
resp_headers_snapshot = dict(response.headers)
```

Build the masked address once, reuse across event calls:

```python
masked_address = self._mask_url_auth_params(url, injected_param_keys)
```

Update the two `DataExchangeEvent(...)` calls inside `request()`:

**Success path:**
```python
await self._emit_data_exchange(
    DataExchangeEvent(
        ...existing fields...,
        address=masked_address,          # replaces self._mask_sensitive_url(url)
        request_headers=req_headers_snapshot,
        response_headers=resp_headers_snapshot,
    ),
    correlation=None,
)
```

**Timeout/connect-error (final failure) path:**
```python
await self._emit_data_exchange(
    DataExchangeEvent(
        ...existing fields...,
        address=masked_address,
        request_headers=req_headers_snapshot,
        # no response headers on connection failure
    ),
    correlation=None,
)
```

#### 4g. Replace all `await record_data_exchange(...)` in `HttpClient` with `await self._emit_data_exchange(...)`

Two call sites in `HttpClient.request()` (success path, failure path) — both replaced.

---

## Masking Format Summary

| Data | Exempt from masking | Masked Format |
|---|---|---|
| Header values | `content-type` | `first3...last3` (or `***` if ≤ 6 chars) |
| URL query-param values | params NOT injected by `authorize()` and not in `_SENSITIVE_PARAM_KEYS` | `first3...last3` (or `***` if ≤ 6 chars) |

Examples:
- `Bearer eyJhbGc...` → `Bea...abc`
- `PROJECT-19142` → `PRO...142`
- `abc` (short) → `***`
- A query param like `page=2` (not auth-related) — **not masked**

---

## Data Flow After Change

```
HttpClient.request()
  │
  ├─ injected_param_keys = await self.authorize(headers, params, cookies)
  │     # authorize() returns set of param keys it injected (e.g. {"api_key"})
  │
  ├─ masked_address = self._mask_url_auth_params(url, injected_param_keys)
  │     # masks values of injected params + _SENSITIVE_PARAM_KEYS matches
  │
  ├─ req_headers_snapshot = dict(headers)   # snapshot after auth injection
  │
  ├─ http request ...
  │
  ├─ resp_headers_snapshot = dict(response.headers)
  │
  └─ self._emit_data_exchange(DataExchangeEvent(
         ...,
         address=masked_address,
         request_headers=req_headers_snapshot,
         response_headers=resp_headers_snapshot,
     ))
         │
         ▼  BaseClient._emit_data_exchange
         │
         └─ masked = self.mask_sensitive_data(event)   ← HttpClient override
                 │
                 └─ dataclasses.replace(event,
                        request_headers=mask_all_except_content_type(...),
                        response_headers=mask_all_except_content_type(...),
                        # address already masked at construction time
                    )
                 │
                 ▼
         await record_data_exchange(masked, correlation)
```

---

## Todo List

- [ ] **model.py**: Add `request_headers: Optional[Dict[str, str]] = None` to `DataExchangeEvent`
- [ ] **model.py**: Add `response_headers: Optional[Dict[str, str]] = None` to `DataExchangeEvent`
- [ ] **model.py**: Add `request_headers: Optional[Dict[str, str]] = None` to `DataExchange`
- [ ] **model.py**: Add `response_headers: Optional[Dict[str, str]] = None` to `DataExchange`
- [ ] **ingestion.py**: Pass `request_headers=event.request_headers` and `response_headers=event.response_headers` in `DataExchange(...)` inside `_process_dx`
- [ ] **base.py**: Import `DataExchangeEvent`, `Correlation`, `record_data_exchange`, `Optional`
- [ ] **base.py**: Add `mask_sensitive_data(self, event) -> DataExchangeEvent` (pass-through default)
- [ ] **base.py**: Add `async _emit_data_exchange(self, event, correlation=None)` calling `mask_sensitive_data` then `record_data_exchange`
- [ ] **http.py**: Add module-level `_mask_value(val: str) -> str` helper
- [ ] **http.py**: Change `authorize()` return type from `None` to `set[str]`; return `injected_param_keys` (add auth-injected query-param key to the set)
- [ ] **http.py**: Update the `authorize()` call in `request()` to capture `injected_param_keys = await self.authorize(...)`; also update the re-authorize call inside the retry loop (discard return value there)
- [ ] **http.py**: Add `_mask_url_auth_params(self, url, auth_param_keys)` instance method replacing `_mask_sensitive_url`; masks values of `auth_param_keys` union `_SENSITIVE_PARAM_KEYS` using `_mask_value`
- [ ] **http.py**: Remove `_mask_sensitive_url`; update `_to_curl_command` if it references it (it has its own masking logic — verify and keep as-is)
- [ ] **http.py**: Add `_UNMASKED_HEADER_KEYS = frozenset(["content-type"])` constant
- [ ] **http.py**: Override `mask_sensitive_data` in `HttpClient` using `dataclasses.replace` — masks headers only (address already correct)
- [ ] **http.py**: In `request()`, compute `masked_address` and `req_headers_snapshot` after `authorize()`
- [ ] **http.py**: In `request()` success path, add `address=masked_address`, `request_headers`, `response_headers` to `DataExchangeEvent` and use `self._emit_data_exchange`
- [ ] **http.py**: In `request()` failure path (connection error), add `address=masked_address`, `request_headers` to `DataExchangeEvent` and use `self._emit_data_exchange`
- [ ] **http.py**: Keep `record_data_exchange` import (still used by `OAuth2Manager` directly)


**Summary**
- **Issue**: When OAuth2 token refresh fails (3x) inside `OAuth2Manager.refresh_token`, the exception propagates up through `authorize()` and is re-raised at line 580 of `HttpClient.request()`. That `raise` exits `request()` entirely — **before** `start_time` is assigned and before the retry loop's observability recording is ever reached. As a result, **no observability event is written for the main HTTP request in that failed invocation**. The SUCCEEDED event visible in the UI is from a prior successful call that was never overwritten by a FAILED counterpart.

**Goal**
- Ensure a FAILED observability event is emitted for the main HTTP request whenever `authorize()` raises before the request is even sent.

**Relevant files / functions**
- File: [packages/flowstash_clients/src/flowstash/clients/http.py](packages/flowstash_clients/src/flowstash/clients/http.py)
  - `HttpClient.request()` — lines 573-580: auth failure early-exit path missing observability.
  - `class OAuth2Manager` — `refresh_token` — already records events per token endpoint attempt; this part is working correctly.
- Observability helper used: `record_data_exchange` (imported at top of same file).

**Root cause (confirmed)**
In `HttpClient.request()`:
```
# line 573
try:
    await self.authorize(headers, params, cookies)  # raises after 3x token failures
except Exception as auth_exc:
    logger.error(...)
    raise  # line 580 — exits method; start_time never set; observability never recorded
```
The retry loop and all `record_data_exchange` calls are placed AFTER line 580, so auth failures produce zero observability events for the main request. The old SUCCEEDED record from a previous run remains as the last known state for that operation.

**The fix (single change)**

In `HttpClient.request()`, record a FAILED event for the main request before re-raising the auth exception. `start_time` must be captured before the `authorize()` call so it's available in the `except` block.

Before:
```python
        # Additional Auth Handling
        try:
            await self.authorize(headers, params, cookies)
        except Exception as auth_exc:
            logger.error(
                f"Authorization failed for {self.name} [{method} {url}]: {auth_exc}"
            )
            raise
        
        # ...
        start_time = datetime.now(UTC)
```

After:
```python
        start_time = datetime.now(UTC)  # moved up, before authorize()

        # Additional Auth Handling
        try:
            await self.authorize(headers, params, cookies)
        except Exception as auth_exc:
            logger.error(
                f"Authorization failed for {self.name} [{method} {url}]: {auth_exc}"
            )
            await record_data_exchange(
                DataExchangeEvent(
                    integration=self.settings.client_id,
                    channel="HTTP",
                    operation=f"{method} {path}",
                    remote_system=self.base_url,
                    address=self._mask_sensitive_url(url),
                    occurred_at=start_time,
                    completed_at=datetime.now(UTC),
                    state="FAILED",
                    attempt=1,
                    http_method=method,
                    attrs={"error": f"Authorization failed: {auth_exc}"},
                ),
                correlation=None,
            )
            raise
        
        # ...
        # start_time already set above
```

Note: `req_body_bytes` / `req_content_type` extraction happens after auth (it's not worth moving it before since auth failure means no request was sent anyway — omitting request payload from this event is intentional and correct).

**Acceptance criteria**
- When token refresh fails 3x, observability shows: 3x `OAUTH2_TOKEN_REFRESH FAILED` + 1x `/demo/process_user FAILED` (with attrs describing the auth error). No stale SUCCEEDED is left as the last-known state.

**TODO**
1. Move `start_time = datetime.now(UTC)` up above the `authorize()` try/except in `HttpClient.request()`.
2. Add `record_data_exchange(DataExchangeEvent(state="FAILED", ...))` inside the `except Exception as auth_exc:` block, before `raise`.
3. Add a similar guard for the re-auth path inside the retry loop (lines ~630-640) — same pattern, same fix: record FAILED before re-raising `re_auth_exc`.
4. Add a unit test: mock `_auth_manager.get_token` to raise `ConnectError`, call `client.request(...)`, assert `record_data_exchange` was called once with `state="FAILED"` and `operation="POST /demo/process_user"`.

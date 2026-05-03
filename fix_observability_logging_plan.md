# Plan: Fix Standard Python `logging` → Observability Capture

**File**: `packages/flowstash_lib/src/flowstash/observability/logging.py`  
**Status**: Not implemented — only planning

---

## Problem Statement

Any call made via the standard Python `logging` module (e.g. `logging.getLogger("my.module").info("msg")`) should be captured and forwarded to observability when execution is inside an `IntegrationContext`. This is currently broken.

---

## Root Cause Analysis

### Bug 1 — Root logger level silently drops records (PRIMARY) ✅ CONFIRMED

**Location**: `setup_global_logging()` in `logging.py`

**Confirmed**: `set_observability_config` is called with `LoggingConfig(enabled=True, min_level='INFO')`, yet INFO logs from `logging.getLogger("flowstash.clients.http")` in `http.py` are never captured. The handler is installed, the config is correct — records simply never reach it.

`setup_global_logging()` correctly installs an `IntegrationLogHandler` on the root logger:
```python
handler = IntegrationLogHandler()
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)
```

However, Python's logging framework evaluates `logger.isEnabledFor(level)` **before creating a log record**. The check walks up the logger hierarchy to find the first non-zero level:

```
logging.getLogger("flowstash.clients.http")  → level=NOTSET (0)
  └── flowstash.clients                       → level=NOTSET (0)
       └── flowstash                          → level=NOTSET (0)
            └── root logger                  → level=WARNING (30)   ← default
```

So calling `.info(...)` on any child logger with no explicit level set triggers `isEnabledFor(INFO=20) → False` (because effective level is WARNING=30). The `LogRecord` is **never created**. The `IntegrationLogHandler.emit()` is never called, regardless of the handler's own level setting.

**Evidence**: `logging.getLogger().setLevel(logging.WARNING)` is the Python default, set at `root = RootLogger(WARNING)` in CPython's `logging/__init__.py`. The handler's own `setLevel(DEBUG)` only filters records that *reach* the handler — it cannot un-block records dropped before creation.

---

### Bug 2 — `self.format(record)` produces formatted log line, not raw message

**Location**: `IntegrationLogHandler.emit()` in `logging.py`

```python
msg = self.format(record)  # ← produces "INFO:module.name:the actual message"
```

`self.format(record)` invokes the handler's formatter (defaulting to `logging.BASIC_FORMAT`), producing a string like:
```
INFO:flowstash.clients.http:Refreshing OAuth2 token for client_id via https://...
```

But `enqueue_log_event()` expects just the raw message string — consistent with how `IntegrationLogger._emit()` works (`msg % args`). The correct call is `record.getMessage()` which returns only the formatted message text (with `%s` substitutions applied, no level/name prefix).

---

### Bug 3 — `exc_info` false-positive for records with no active exception

**Location**: `IntegrationLogHandler.emit()` in `logging.py`

```python
exc_info=bool(record.exc_info)   # ← BUG
```

When a logger is called inside an `except` block with `exc_info=True` but the exception tuple has already been cleared, `record.exc_info` can be `(None, None, None)`. A non-empty tuple is truthy in Python:

```python
bool((None, None, None))  # True ← wrong
```

The correct guard is `record.exc_info is not None and record.exc_info[0] is not None`.

---

## Approaches to Fix Bug 1

Three approaches to fix the root logger level problem:

### Approach A — Lower root logger level + preserve existing handler levels (RECOMMENDED)

**How**: In `setup_global_logging()`:
1. For each existing handler on the root logger that has level `NOTSET (0)`, explicitly set its level to `WARNING (30)` — this preserves its previous effective filtering behaviour (which was inherited from the root's WARNING level).
2. Set root logger level to `NOTSET (0)` — this allows all records to be created and propagated.

```python
root_logger = logging.getLogger()
# Preserve existing handlers: without this, they would start emitting
# DEBUG/INFO that was previously blocked by the root logger's WARNING level.
for existing_handler in root_logger.handlers:
    if existing_handler.level == logging.NOTSET:
        existing_handler.setLevel(logging.WARNING)

root_logger.addHandler(handler)
root_logger.setLevel(logging.NOTSET)  # allow all records through; our handler filters
```

**Pros**:
- Fixes the bug correctly
- Existing console/file handlers maintain their previous effective log level (no unexpected console flooding)
- Single-function change, no invasive modifications

**Cons**:
- Mutates the level of existing handlers — might surprise users who intentionally set a handler to NOTSET expecting it to defer to root
- Handler mutation is a one-time side effect at setup time

---

### Approach B — Set root logger to DEBUG only (minimal/risky)

**How**: Just add `logging.getLogger().setLevel(logging.DEBUG)` before adding the handler.

**Pros**: Minimal code change

**Cons**:
- If the application has existing root handlers (e.g. from `logging.basicConfig()`) with NOTSET level, they will start emitting DEBUG/INFO logs to console — potential flooding
- No protection for existing handler levels

---

### Approach C — Use a `logging.Filter` to bypass level on existing handlers

**How**: Add a filter to existing root handlers that re-applies their previous effective level.

**Pros**: Technically clean

**Cons**:
- Complex: requires custom `Filter` subclass that captures effective level at setup time
- `Filter` is designed for routing decisions, not for re-applying level checks — non-idiomatic
- Fragile: handlers added after `setup_global_logging()` would be unprotected

---

### Recommendation: Approach A

Approach A is the most correct and safe. It models exactly what we want: "let all records through at the root, but ensure other handlers aren't affected". The one-time mutation of existing handler levels is a well-defined, deterministic side effect.

---

## Summary of All Changes

### File: `packages/flowstash_lib/src/flowstash/observability/logging.py`

#### Change 1 — `IntegrationLogHandler.emit()`: fix message extraction and exc_info

```python
# Before:
msg = self.format(record)
enqueue_log_event(
    logger_name=record.name,
    levelno=record.levelno,
    message=msg,
    attrs=getattr(record, "attrs", None),
    exc_info=bool(record.exc_info)
)

# After:
msg = record.getMessage()
enqueue_log_event(
    logger_name=record.name,
    levelno=record.levelno,
    message=msg,
    attrs=getattr(record, "attrs", None),
    exc_info=record.exc_info is not None and record.exc_info[0] is not None,
)
```

#### Change 2 — `setup_global_logging()`: lower root logger level + protect existing handlers

```python
# Before:
handler = IntegrationLogHandler()
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

# After:
handler = IntegrationLogHandler()
handler.setLevel(logging.NOTSET)  # handler accepts all; enqueue_log_event does min_level filtering

root_logger = logging.getLogger()
# Existing handlers with NOTSET level would start emitting everything once we lower
# root to NOTSET below. Preserve their previous effective behaviour (WARNING).
for existing_handler in root_logger.handlers:
    if existing_handler.level == logging.NOTSET:
        existing_handler.setLevel(logging.WARNING)

root_logger.addHandler(handler)
# Lower root level so all child loggers can create records and propagate to our handler.
# The min_level filter in enqueue_log_event() / ObservabilityConfig controls what is stored.
root_logger.setLevel(logging.NOTSET)
```

---

## Affected Files

| File | Change |
|------|--------|
| `packages/flowstash_lib/src/flowstash/observability/logging.py` | Changes 1 and 2 above |
| `packages/flowstash_lib/src/flowstash/observability/ingestion.py` | No change needed |
| `packages/flowstash_lib/tests/test_logger.py` | Add test for global handler path (new test) |

---

## New Test to Add in `test_logger.py`

A test that exercises the `IntegrationLogHandler` path (i.e. a standard `logging.getLogger()` call inside a context, without using the `IntegrationLogger` shim):

```python
def test_global_logging_handler_captures_standard_logger():
    """
    Standard logging.getLogger() calls inside an integration_context must be
    forwarded to observability via the root handler path (not IntegrationLogger).
    """
    setup_global_logging()  # ensure handler is installed

    std_logger = logging.getLogger("test.some.external.module")

    with integration_context(integration="test-int"):
        with patch("flowstash.observability.logging.enqueue_log_event") as mock_enqueue:
            std_logger.info("standard log from external module")

            mock_enqueue.assert_called_once()
            _, k = mock_enqueue.call_args
            assert k["logger_name"] == "test.some.external.module"
            assert k["levelno"] == logging.INFO
            assert k["message"] == "standard log from external module"
            assert k["exc_info"] is False


def test_global_handler_exc_info_false_for_no_exception():
    """
    Records without an active exception must not set exc_info=True.
    (Regression for the bool((None, None, None)) bug.)
    """
    setup_global_logging()
    std_logger = logging.getLogger("test.exc_info_check")

    with integration_context(integration="test-int"):
        with patch("flowstash.observability.logging.enqueue_log_event") as mock_enqueue:
            std_logger.info("no exception here")

            _, k = mock_enqueue.call_args
            assert k["exc_info"] is False
```

---

## Todo

- [ ] **Bug 1 (primary)**: In `setup_global_logging()`, iterate existing root handlers with NOTSET level → set them to WARNING, then set root logger level to NOTSET
- [ ] **Bug 2**: In `IntegrationLogHandler.emit()`, replace `self.format(record)` with `record.getMessage()`
- [ ] **Bug 3**: In `IntegrationLogHandler.emit()`, replace `bool(record.exc_info)` with `record.exc_info is not None and record.exc_info[0] is not None`
- [ ] **Tests**: Add `test_global_logging_handler_captures_standard_logger` and `test_global_handler_exc_info_false_for_no_exception` to `test_logger.py`

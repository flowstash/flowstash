import logging
import os
import threading
from typing import Any, Dict, Optional, Union
from ..context import get_context
from .ingestion import enqueue_log_event, get_observability_config

# Thread-local flag to prevent double-capture when IntegrationLogger passes through
# to the underlying Python logger while IntegrationLogHandler is also installed.
_in_integration_logger = threading.local()


def _passthrough_enabled() -> bool:
    """Check whether captured logs should also be forwarded to stdout/Python logging.

    Priority: FLOWSTASH_LOG_PASSTHROUGH env var > observability config > default True.
    """
    env = os.getenv("FLOWSTASH_LOG_PASSTHROUGH")
    if env is not None:
        return env.lower() not in ("false", "0", "no")
    try:
        return get_observability_config().logging.passthrough
    except Exception:
        return True


class IntegrationLogger:
    """
    A logger shim that routes logs to observability when in an IntegrationContext,
    and to standard Python logging otherwise.
    """
    def __init__(self, python_logger: logging.Logger):
        self._python_logger = python_logger

    @property
    def name(self) -> str:
        return self._python_logger.name

    def debug(self, msg: str, *args, **kwargs):
        self._emit(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self._emit(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self._emit(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self._emit(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        # Behaves like python logging: ERROR with exc_info=True
        kwargs["exc_info"] = True
        self._emit(logging.ERROR, msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs):
        self._emit(level, msg, *args, **kwargs)

    def _emit(self, level: int, msg: str, *args, **kwargs):
        """
        Routing logic:
        1. If in-context -> capture to observability
        2. Otherwise -> passthrough to python logging
        3. Never blocks or throws
        """
        ctx = get_context()
        
        if ctx is None:
            self._python_logger.log(level, msg, *args, **kwargs)
            return

        try:
            # Format message if args provided (logger.info("x=%s", x))
            formatted_msg = msg
            if args:
                try:
                    formatted_msg = msg % args
                except (TypeError, ValueError):
                    # Guard against formatting errors, use original msg
                    pass

            # Extract attributes from kwargs
            # We want to capture additional keyword arguments as attributes
            standard_kwargs = {"exc_info", "stack_info", "stacklevel", "extra"}
            attrs = {k: v for k, v in kwargs.items() if k not in standard_kwargs}
            
            # Merge 'extra' if present
            extra = kwargs.get("extra")
            if isinstance(extra, dict):
                attrs.update(extra)

            enqueue_log_event(
                logger_name=self._python_logger.name,
                levelno=level,
                message=formatted_msg,
                attrs=attrs,
                exc_info=bool(kwargs.get("exc_info"))
            )

            if _passthrough_enabled():
                _in_integration_logger.active = True
                try:
                    self._python_logger.log(level, msg, *args, **kwargs)
                finally:
                    _in_integration_logger.active = False
        except Exception:
            # Observability must never fail execution
            pass

# Create singleton
logger = IntegrationLogger(logging.getLogger("flowstash.user"))

_global_logging_setup = False

def setup_global_logging():
    global _global_logging_setup
    if _global_logging_setup:
        return
    _global_logging_setup = True
    
    import logging
    import sys

    class IntegrationLogHandler(logging.Handler):
        def emit(self, record):
            # Prevent infinite loops and only capture when inside a context
            if get_context() is None or record.name.startswith("flowstash.observability"):
                return
            # Already captured by IntegrationLogger._emit(); skip to avoid double-ingestion
            if getattr(_in_integration_logger, "active", False):
                return

            try:
                enqueue_log_event(
                    logger_name=record.name,
                    levelno=record.levelno,
                    message=record.getMessage(),
                    attrs=getattr(record, "attrs", None),
                    exc_info=record.exc_info is not None and record.exc_info[0] is not None,
                )
            except Exception:
                pass

    handler = IntegrationLogHandler()
    handler.setLevel(logging.NOTSET)

    root_logger = logging.getLogger()
    # When passthrough is disabled, pin existing handlers (e.g. the default StreamHandler)
    # to WARNING so they don't start emitting DEBUG/INFO after we lower the root level.
    # When passthrough is enabled we leave them untouched — they need to keep receiving
    # records so that captured logs also appear on stdout.
    if not _passthrough_enabled():
        for existing_handler in root_logger.handlers:
            if existing_handler.level == logging.NOTSET:
                existing_handler.setLevel(logging.WARNING)

    root_logger.addHandler(handler)
    # Allow all records to be created and reach our handler.
    # enqueue_log_event() applies the min_level / prefix filters from ObservabilityConfig.
    root_logger.setLevel(logging.NOTSET)
    
    class IntegrationStreamProxy:
        def __init__(self, original_stream, levelno):
            self.original_stream = original_stream
            self.levelno = levelno
            self.logger_name = "stdout" if levelno == logging.INFO else "stderr"

        def write(self, data):
            self.original_stream.write(data)
            from ..context import get_context
            if get_context() is not None:
                text = data.strip()
                if text:
                    from .ingestion import enqueue_log_event
                    try:
                        enqueue_log_event(
                            logger_name=self.logger_name,
                            levelno=self.levelno,
                            message=text
                        )
                    except Exception:
                        pass

        def flush(self):
            self.original_stream.flush()

        def __getattr__(self, name):
            return getattr(self.original_stream, name)
            
    sys.stdout = IntegrationStreamProxy(sys.stdout, logging.INFO)
    sys.stderr = IntegrationStreamProxy(sys.stderr, logging.ERROR)

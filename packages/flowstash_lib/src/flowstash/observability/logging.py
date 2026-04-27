import logging
from typing import Any, Dict, Optional, Union
from ..context import get_context
from .ingestion import enqueue_log_event


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
        except Exception:
            # Observability must never fail execution
            pass

# Create singleton
logger = IntegrationLogger(logging.getLogger("flowstash.user"))

from __future__ import annotations
import contextvars
from contextlib import contextmanager
from typing import Any, Optional, TYPE_CHECKING, Literal

from ..context import IntegrationContext, current_context, _state_handle
from ..state.protocol import StateStoreProtocol, _decode_entry

if TYPE_CHECKING:
    from ..state.entry import StateEntry

class State:
    """
    Facade for interacting with state stores.
    """

    @staticmethod
    @contextmanager
    def use(ctx: IntegrationContext):
        """Context manager to bind a specific integration context to the state facade."""
        handle = StateHandle(ctx)
        token = _state_handle.set(handle)
        try:
            yield
        finally:
            _state_handle.reset(token)

    @staticmethod
    def current() -> StateHandle:
        """Returns a bound handle for the current context."""
        # Check explicit state handle context first
        handle = _state_handle.get()
        if handle:
            return handle
            
        # Fallback to general integration context and wrap it on the fly
        ctx = current_context()
        if not ctx:
            raise RuntimeError(
                "State used outside of an integration run. Use `with State.use(ctx): ...`"
            )
        
        # We don't cache it back into _state_handle here to avoid modifying
        # the context indefinitely if it wasn't explicitly 'use'd, 
        # but we could. Actually, for performance in loops, we should probably
        # ensure it's stable.
        return StateHandle(ctx)

    @staticmethod
    def get_entry(key: str, scope: Literal["integration", "pipeline", "ingress"] = "integration") -> Optional[StateEntry]:
        """Resolve context and call underlying get_entry."""
        return State.current().get_entry(key, scope=scope)

    @staticmethod
    def get(key: str, scope: Literal["integration", "pipeline", "ingress"] = "integration") -> Any:
        """Resolve context, call underlying get_entry, and decode."""
        return State.current().get(key, scope=scope)

    @staticmethod
    def set(key: str, value: Any, scope: Literal["integration", "pipeline", "ingress"] = "integration", ttl_s: Optional[int] = None) -> None:
        """Resolve context, encode, and call underlying set."""
        return State.current().set(key, value, scope=scope, ttl_s=ttl_s)

class StateHandle:
    """A bound handle to the state store for a specific context."""

    def __init__(self, ctx: IntegrationContext):
        self._ctx = ctx
        self._store: Optional[StateStoreProtocol] = None

    def _get_store(self) -> StateStoreProtocol:
        if self._store:
            return self._store
        
        # Resolve store based on global configuration if available
        from ..config.runtime_config import get_global_registry
        registry = get_global_registry()
        if registry is not None:
            self._store = registry._config.state_store.build_store()
            return self._store

        # Fallback to SQLite (useful for tests/CLI without full config)
        from ..state.stores.sqlite_store import SQLiteStateStore
        db_path = ".flowstash_state.db"
        # In tests we might still want memory, but for CLI/Local we want a file
        self._store = SQLiteStateStore(db_path=db_path)
        return self._store

    def _resolve_namespace(self, scope: str) -> str:
        if scope == "integration":
            if not self._ctx.integration or self._ctx.integration == "unknown":
                 raise RuntimeError("Scope 'integration' requested but no integration name is present in context.")
            return f"integration:{self._ctx.integration}"
        
        elif scope == "pipeline":
            if not self._ctx.integration_pipeline or self._ctx.integration_pipeline == "unknown":
                 raise RuntimeError("Scope 'pipeline' requested but no pipeline is present in context.")
            return f"pipeline:{self._ctx.integration_pipeline}"
        
        elif scope == "ingress":
            if not self._ctx.ingress_name:
                 raise RuntimeError("Scope 'ingress' requested but not inside an ingress context (ingress_name missing).")
            # We follow the convention: pipeline:{pipeline}:ingress:{ingress_name}
            return f"pipeline:{self._ctx.integration_pipeline}:ingress:{self._ctx.ingress_name}"
        
        else:
            raise ValueError(f"Unknown scope: {scope}")

    def get_entry(self, key: str, scope: str = "integration") -> Optional[StateEntry]:
        namespace = self._resolve_namespace(scope)
        return self._get_store().get_entry(namespace, key)

    def get(self, key: str, scope: str = "integration") -> Any:
        entry = self.get_entry(key, scope=scope)
        if entry is None:
            return None
        return _decode_entry(entry)

    def set(self, key: str, value: Any, scope: str = "integration", ttl_s: Optional[int] = None) -> None:
        namespace = self._resolve_namespace(scope)
        self._get_store().set(namespace, key, value, ttl_s=ttl_s)

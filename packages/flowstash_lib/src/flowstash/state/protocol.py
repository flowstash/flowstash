from __future__ import annotations

import json
from typing import Any, Optional, Protocol, runtime_checkable

from .entry import StateEntry

_MAX_NAMESPACE_LEN = 512
_MAX_KEY_LEN = 512


def _validate(namespace: str, key: str) -> None:
    """Shared validation for namespace and key arguments."""
    if not namespace:
        raise ValueError("namespace must not be empty")
    if not key:
        raise ValueError("key must not be empty")
    if len(namespace) > _MAX_NAMESPACE_LEN:
        raise ValueError(f"namespace exceeds {_MAX_NAMESPACE_LEN} characters")
    if len(key) > _MAX_KEY_LEN:
        raise ValueError(f"key exceeds {_MAX_KEY_LEN} characters")


def _decode_entry(entry: StateEntry) -> Any:
    """Decode a StateEntry's value according to its content_type."""
    ct = entry.content_type.lower().split(";")[0].strip()
    if ct == "application/json":
        return json.loads(entry.value.decode("utf-8"))
    if ct.startswith("text/"):
        return entry.value.decode("utf-8")
    # Binary / unknown — return raw bytes
    return entry.value


@runtime_checkable
class StateStoreProtocol(Protocol):
    """Protocol that all StateStore implementations must satisfy."""

    def get_entry(self, namespace: str, key: str) -> Optional[StateEntry]:
        """
        Return the stored StateEntry, or None if missing or expired.
        Implementations MUST filter out expired entries.
        """
        ...

    def get(self, namespace: str, key: str) -> Optional[Any]:
        """
        Convenience wrapper: returns the decoded value or None.
        Decoding is based on content_type (JSON by default).
        """
        ...

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        content_type: str = "application/json",
        ttl_s: Optional[int] = None,
    ) -> StateEntry:
        """
        Store a value under (namespace, key).
        - ttl_s=None → no expiry (clears any prior TTL)
        - ttl_s=int  → expires after ttl_s seconds from now
        Returns the stored StateEntry.
        """
        ...

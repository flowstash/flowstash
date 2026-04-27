from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class StateEntry:
    """Represents a single stored state value."""

    value: bytes
    content_type: str = "application/json"
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    version: int = 1

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Return True if this entry is past its expiry time."""
        if self.expires_at is None:
            return False
        if now is None:
            now = datetime.now(timezone.utc)
        return self.expires_at <= now

"""Context values that follow work across task hops.

When a task is submitted or scheduled, :func:`extract_propagation` takes the
values from the caller's context that the child run should inherit. They travel
in ``TaskDelegationMetadata.propagated`` through every backend (asyncio,
dramatiq headers, managed platform payload). On the execution side,
:func:`context_kwargs_from_propagation` turns them back into
``integration_context(...)`` arguments and :func:`link_inherited_record` links
the inherited record to the child run.

Backends only call these three helpers, so adding a propagated value later
touches this module alone.
"""

import logging
from typing import Mapping, Optional

from .context import IntegrationContext
from .observability.record_links import link_inherited_record

logger = logging.getLogger(__name__)

PROPAGATED_RECORD_KEY = "fw.record_key"

# Cloud Tasks rejects tasks over 1 MB; observability data must never make a
# submit() fail, so propagated values stay tiny.
MAX_VALUE_BYTES = 512
MAX_TOTAL_BYTES = 4096

__all__ = [
    "PROPAGATED_RECORD_KEY",
    "extract_propagation",
    "context_kwargs_from_propagation",
    "link_inherited_record",
]


def extract_propagation(ctx: Optional[IntegrationContext]) -> dict[str, str]:
    """Values from ``ctx`` that a submitted task should inherit."""
    if ctx is None:
        return {}
    candidates: dict[str, Optional[str]] = {}
    if ctx.propagate_record:
        candidates[PROPAGATED_RECORD_KEY] = ctx.current_record_key

    propagated: dict[str, str] = {}
    total = 0
    for name, value in candidates.items():
        if not isinstance(value, str) or not value:
            continue
        size = len(name.encode()) + len(value.encode())
        if len(value.encode()) > MAX_VALUE_BYTES or total + size > MAX_TOTAL_BYTES:
            logger.warning(
                "Not propagating %s to the submitted task: value too large (%d bytes)",
                name,
                len(value.encode()),
            )
            continue
        propagated[name] = value
        total += size
    return propagated


def context_kwargs_from_propagation(
    propagated: Optional[Mapping[str, str]],
) -> dict:
    """``integration_context(...)`` keyword arguments for inherited values."""
    if not propagated:
        return {}
    kwargs: dict = {}
    record_key = propagated.get(PROPAGATED_RECORD_KEY)
    if isinstance(record_key, str) and record_key:
        kwargs["current_record_key"] = record_key
    return kwargs

"""Label a run with the business records it touched.

Feeds emit ``PUBLISHED`` / ``CONSUMED`` links on their own. Plain tasks, polls
and webhooks that call APIs directly use the helpers here to emit ``LINKED``
links, so the run's Records tab (and record search) shows what they processed.

Public entry points (re-exported from ``flowstash.records``):

- :func:`record_scope` — "this block works on record X": links it and tags
  every log/span inside the block with the record key.
- :func:`link_record` / :func:`link_records` — mark one or many records.

All helpers are synchronous, never block on IO, never raise, and do nothing
outside a run. Each run keeps at most ``FLOWSTASH_RECORD_LINKS_MAX_PER_RUN``
(default 5000) ``LINKED`` links; the same key is linked once per run.
"""

import dataclasses
import logging
import os
import threading
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, UTC
from itertools import islice
from typing import Iterable, Iterator, Optional, Union

from ..context import (
    IntegrationContext,
    current_context,
    integration_context,
    reset_context,
    set_context,
)
from .model import RecordLink, RecordLinkKind

logger = logging.getLogger(__name__)

RecordId = Union[str, int]

DEFAULT_MAX_LINKS_PER_RUN = 5000
MAX_LINKS_ENV = "FLOWSTASH_RECORD_LINKS_MAX_PER_RUN"

# link_records() reads its iterable in chunks of this size, so a generator of
# many ids is never loaded into memory at once.
_CHUNK_SIZE = 500

# Upper bound on live budgets. A run that never reaches its exit (hard kill,
# links emitted after the run ended) would otherwise keep its budget forever.
_MAX_TRACKED_RUNS = 1000

ACCEPTED_STAT = "fw.record_links_accepted"
DROPPED_STAT = "fw.record_links_dropped"


def _max_links_per_run() -> int:
    try:
        return max(0, int(os.getenv(MAX_LINKS_ENV, DEFAULT_MAX_LINKS_PER_RUN)))
    except ValueError:
        return DEFAULT_MAX_LINKS_PER_RUN


@dataclasses.dataclass
class _RunLinkBudget:
    """Per-run limit and dedupe for LINKED record links. Thread-safe."""

    limit: int
    accepted: int = 0
    dropped: int = 0
    seen: set = dataclasses.field(default_factory=set)
    warned: bool = False
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def take(self, keys: Iterable[str]) -> tuple[list[str], bool]:
        """Return (keys to link, whether this call hit the limit for the first time)."""
        accepted: list[str] = []
        first_drop = False
        with self.lock:
            for key in keys:
                if key in self.seen:
                    continue
                if self.accepted >= self.limit:
                    self.dropped += 1
                    if not self.warned:
                        self.warned = True
                        first_drop = True
                    continue
                self.seen.add(key)
                self.accepted += 1
                accepted.append(key)
        return accepted, first_drop

    def stats(self) -> dict:
        with self.lock:
            return {ACCEPTED_STAT: self.accepted, DROPPED_STAT: self.dropped}


_budgets: "OrderedDict[str, _RunLinkBudget]" = OrderedDict()
_budgets_lock = threading.Lock()


def _budget_for(run_id: str) -> _RunLinkBudget:
    with _budgets_lock:
        budget = _budgets.get(run_id)
        if budget is None:
            budget = _RunLinkBudget(limit=_max_links_per_run())
            _budgets[run_id] = budget
            while len(_budgets) > _MAX_TRACKED_RUNS:
                _budgets.popitem(last=False)
        return budget


def peek_link_stats(run_id: str) -> Optional[dict]:
    """Link counts for a live run, or None if the run linked nothing."""
    with _budgets_lock:
        budget = _budgets.get(run_id)
    return budget.stats() if budget is not None else None


def release_link_budget(run_id: str) -> Optional[dict]:
    """Forget a finished run's budget and return its final link counts."""
    with _budgets_lock:
        budget = _budgets.pop(run_id, None)
    return budget.stats() if budget is not None else None


def _build_key(
    ctx: IntegrationContext,
    record_type: str,
    record_id: RecordId,
    key: Optional[str] = None,
) -> str:
    """Same format as feed dedupe keys, so links join across integrations."""
    if key:
        return key
    return f"{ctx.integration}:{record_type}:{record_id}"


def _is_blank(record_id: Optional[RecordId]) -> bool:
    return record_id is None or (isinstance(record_id, str) and not record_id.strip())


def _emit(ctx: IntegrationContext, keys: list[str], source: Optional[str]) -> int:
    """Apply the run budget to keys and emit LINKED links for the ones accepted."""
    if not keys:
        return 0
    budget = _budget_for(ctx.run_id)
    accepted, first_drop = budget.take(keys)
    if first_drop:
        logger.warning(
            "Record link limit %d reached for run %s; further record links are dropped",
            budget.limit,
            ctx.run_id,
        )
    if not accepted:
        return 0

    from .ingestion import emit_record_links

    now = datetime.now(UTC)
    emit_record_links(
        [
            RecordLink(
                tenant_id=ctx.tenant_id,
                integration=ctx.integration,
                pipeline=ctx.integration_pipeline,
                run_id=ctx.run_id,
                trace_id=ctx.trace_id,
                span_id=ctx.span_id,
                record_key=key,
                event_time=now,
                kind=RecordLinkKind.LINKED,
                source=source,
            )
            for key in accepted
        ]
    )
    return len(accepted)


def link_record(
    record_id: RecordId,
    *,
    record_type: str,
    key: Optional[str] = None,
    source: Optional[str] = None,
) -> None:
    """Mark the current run as having touched one record.

    Args:
        record_id: The record's id in its source system (e.g. a UPC or order id).
        record_type: The kind of record (e.g. ``"upc"``, ``"order"``).
        key: Full record key. Overrides the default
            ``"{integration}:{record_type}:{record_id}"``.
        source: Optional free-form origin shown on the link.

    The link is buffered and sent in the background. Does nothing outside a
    run or for an empty id. Never raises.
    """
    try:
        ctx = current_context()
        if ctx is None:
            logger.debug("link_record called outside a run; ignored")
            return
        if _is_blank(record_id) and not key:
            return
        _emit(ctx, [_build_key(ctx, record_type, record_id, key)], source)
    except Exception:
        logger.debug("link_record failed", exc_info=True)


def link_records(
    record_ids: Iterable[RecordId],
    *,
    record_type: str,
    source: Optional[str] = None,
) -> int:
    """Mark the current run as having touched many records.

    Args:
        record_ids: Any iterable of ids, including a generator. Read in chunks,
            never loaded into memory at once. Empty / ``None`` ids are skipped.
        record_type: The kind of record (e.g. ``"upc"``).
        source: Optional free-form origin shown on the links.

    Returns:
        How many new links were accepted. Ids already linked in this run and
        ids beyond the per-run limit are not counted.

    Unlike :func:`record_scope`, this does not set a current record and does
    not pass anything to tasks submitted afterwards. Never raises.
    """
    try:
        ctx = current_context()
        if ctx is None:
            logger.debug("link_records called outside a run; ignored")
            return 0
        accepted = 0
        ids = iter(record_ids)
        while True:
            chunk = list(islice(ids, _CHUNK_SIZE))
            if not chunk:
                return accepted
            keys = [
                _build_key(ctx, record_type, record_id)
                for record_id in chunk
                if not _is_blank(record_id)
            ]
            accepted += _emit(ctx, keys, source)
    except Exception:
        logger.debug("link_records failed", exc_info=True)
        return 0


def link_inherited_record(ctx: Optional[IntegrationContext]) -> None:
    """Link the record a delegated run inherited from the run that submitted it.

    Called by task backends right after they open the child run's context.
    """
    try:
        if ctx is None or not ctx.current_record_key:
            return
        source = f"run:{ctx.parent_run_id}" if ctx.parent_run_id else "inherited"
        _emit(ctx, [ctx.current_record_key], source)
    except Exception:
        logger.debug("link_inherited_record failed", exc_info=True)


@contextmanager
def record_scope(
    record_id: RecordId,
    *,
    record_type: str,
    key: Optional[str] = None,
    span: bool = False,
    propagate: bool = True,
) -> Iterator[Optional[str]]:
    """Mark a block of code as working on one record.

    On enter, links the record to the current run and makes it the current
    record: logs and spans inside the block carry ``fw.record_key``, and tasks
    submitted inside the block start with the same current record.

    Args:
        record_id: The record's id in its source system.
        record_type: The kind of record (e.g. ``"order"``).
        key: Full record key. Overrides the default
            ``"{integration}:{record_type}:{record_id}"``.
        span: Open a span named ``"{record_type}:{record_id}"`` for the block.
            Off by default: thousands of items would create thousands of spans.
        propagate: Pass the record to tasks submitted inside the block. Set to
            False when those tasks are not about this record.

    Yields:
        The full record key, or None outside a run.

    The record is linked on enter, not on success, so failed items show up
    too. Past the per-run link limit the block still tags logs; it just adds
    no new link. Safe under ``asyncio.gather``: each task has its own copy of
    the context.
    """
    ctx = current_context()
    if ctx is None or (_is_blank(record_id) and not key):
        if ctx is None:
            logger.debug("record_scope used outside a run; ignored")
        yield key
        return

    record_key = _build_key(ctx, record_type, record_id, key)

    if span:
        with integration_context(
            span_name=f"{record_type}:{record_id}",
            current_record_key=record_key,
            propagate_record=propagate,
        ):
            link_record(record_id, record_type=record_type, key=record_key)
            yield record_key
        return

    link_record(record_id, record_type=record_type, key=record_key)
    token = set_context(
        dataclasses.replace(
            ctx, current_record_key=record_key, propagate_record=propagate
        )
    )
    try:
        yield record_key
    finally:
        reset_context(token)

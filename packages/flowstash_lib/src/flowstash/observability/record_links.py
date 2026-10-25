"""Label a run with the business records it touched.

Feeds emit ``PUBLISHED`` / ``CONSUMED`` links on their own. Plain tasks, polls
and webhooks that call APIs directly use the helpers here to emit ``LINKED``
links, so the run's Records tab (and record search) shows what they processed.

Public entry points (re-exported from ``flowstash.records``):

- :func:`record_scope` — "this block works on record X": links it and tags
  every log/span inside the block with the record key.
- :func:`link_record` / :func:`link_records` — mark one or many records.

All helpers are synchronous, never block on IO, never raise, and do nothing
outside a run. Only :func:`record_scope` touches the context, and it holds a
single record key, so tasks submitted afterwards inherit at most one key.
"""

import dataclasses
import logging
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

# link_records() reads its iterable in chunks of this size, so a generator of
# many ids is never loaded into memory at once.
_CHUNK_SIZE = 500


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
    """Emit one LINKED link per key for the current run."""
    if not keys:
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
            for key in keys
        ]
    )
    return len(keys)


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
        How many links were emitted (empty / ``None`` ids are not counted).

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
    too. Safe under ``asyncio.gather``: each task has its own copy of
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

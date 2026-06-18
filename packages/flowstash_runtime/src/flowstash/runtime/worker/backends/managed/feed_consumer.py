"""
Feed Consumer — Cloud Run Job helpers for feed-based command dispatch.

Handles two commands dispatched by ManagedConsumer.start():

  consume-feed <base64_encoded_envelope>
      Decodes the classic feed envelope in-process, resolves the @feed_consumer
      handler registered for (feed_id, group_name), and delivers the record.

  consume-feed-batch <batch_id>
      Fetches the batch from the Managed API, delivers items to the handler, and
      acks results (success/failed keys) back to the API.

In both cases the function exits the process directly (sys.exit).
"""

import asyncio
import base64
import json
import logging
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, List, Optional, Tuple

import httpx

from flowstash.context import integration_context
from flowstash.observability.ingestion import AsyncManager
from flowstash.pipelines.record_serialization import from_jsonable

logger = logging.getLogger(__name__)


# ─── Data model ─────────────────────────────────────────────────────


@dataclass
class ClassicFeedEnvelope:
    tenant_id: str
    project_id: str
    environment: str
    feed_id: str
    group_name: str
    dedupe_key: str
    timestamp: float
    data: Any = None
    blob_ref: Optional[str] = None
    record_type: Optional[str] = None
    record_id: Optional[str] = None
    source_integration: Optional[str] = None
    source_pipeline: Optional[str] = None
    source_run_id: Optional[str] = None
    source_traceparent: Optional[str] = None


# ─── Helpers ─────────────────────────────────────────────────────────


def _get_api_config() -> Tuple[str, str]:
    """Return (api_url, api_key) from environment variables."""
    api_url = (
        os.getenv("FLOWSTASH_API_URL")
        or os.getenv("MANAGED_API_URL")
        or "https://api.flowstash.dev"
    ).rstrip("/")
    api_key = os.getenv("FLOWSTASH_API_KEY") or os.getenv("MANAGED_AUTH_TOKEN", "")
    return api_url, api_key


def _decode_envelope(encoded: str) -> ClassicFeedEnvelope:
    """
    Base64-decode + JSON-parse the encoded envelope string.
    Raises ValueError on bad base64, bad JSON, or missing required fields.
    """
    try:
        raw = base64.b64decode(encoded)
    except Exception as e:
        raise ValueError(f"Bad base64 in envelope: {e}") from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Bad JSON in envelope: {e}") from e

    required = (
        "tenant_id",
        "project_id",
        "environment",
        "feed_id",
        "group_name",
        "dedupe_key",
        "timestamp",
    )
    missing = [f for f in required if f not in data]
    if missing:
        raise ValueError(f"Envelope missing required fields: {missing}")

    return ClassicFeedEnvelope(
        tenant_id=data["tenant_id"],
        project_id=data["project_id"],
        environment=data["environment"],
        feed_id=data["feed_id"],
        group_name=data["group_name"],
        dedupe_key=data["dedupe_key"],
        timestamp=data["timestamp"],
        data=data.get("data"),
        blob_ref=data.get("blob_ref"),
        record_type=data.get("record_type"),
        record_id=data.get("record_id"),
        source_integration=data.get("source_integration"),
        source_pipeline=data.get("source_pipeline"),
        source_run_id=data.get("source_run_id"),
        source_traceparent=data.get("source_traceparent"),
    )


def _resolve_feed_consumer(feed_id: str, group_name: str):
    """
    Return the ConsumerSpec registered for (feed_id, group_name).
    Raises ValueError if not found.
    """
    from flowstash.pipelines.consumer import get_registered_consumers

    spec = next(
        (
            c
            for c in get_registered_consumers()
            if c.feed_id == feed_id and c.subscription_name == group_name
        ),
        None,
    )
    if spec is None:
        raise ValueError(
            f"No @feed_consumer registered for feed_id={feed_id!r} group_name={group_name!r}"
        )
    return spec


async def _resolve_record_from_item(item: dict):
    """
    Build a RecordData from an item dict.
    Handles blob_ref resolution: if blob_ref is present and data is None,
    fetches the blob from BlobStore.
    Returns None if the data cannot be resolved.
    """
    from flowstash.pipelines.records_model import RecordData

    data = item.get("data")
    blob_ref = item.get("blob_ref")

    if blob_ref and data is None:
        try:
            from flowstash.observability.registry import get_blob_store

            blob_bytes = await asyncio.to_thread(get_blob_store().get, blob_ref)
            data = json.loads(blob_bytes)
        except Exception as e:
            logger.error(
                f"Failed to resolve blob_ref={blob_ref} for "
                f"dedupe_key={item.get('dedupe_key')}: {e}"
            )
            return None

    ts = item.get("timestamp")
    return RecordData(
        record_id=item.get("record_id") or item.get("dedupe_key"),
        record_type=item.get("record_type") or "managed",
        data=from_jsonable(data),
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else None,
        dedupe_key=item.get("dedupe_key"),
        source_integration=item.get("source_integration"),
        source_pipeline=item.get("source_pipeline"),
        source_run_id=item.get("source_run_id"),
        source_traceparent=item.get("source_traceparent"),
    )


async def _fetch_batch(batch_id: str, api_url: str, api_key: str) -> dict:
    """GET {api_url}/v1/feed/runs/{batch_id}. Returns parsed JSON body."""
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    ) as client:
        resp = await client.get(f"{api_url}/v1/feed/runs/{batch_id}")
        resp.raise_for_status()
        return resp.json()


async def _ack_batch(
    batch_id: str,
    success_keys: List[str],
    failed_keys: List[str],
    failure_reason: Optional[str],
    api_url: str,
    api_key: str,
) -> None:
    """POST {api_url}/v1/feed/runs/{batch_id}/ack."""
    async with httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    ) as client:
        resp = await client.post(
            f"{api_url}/v1/feed/runs/{batch_id}/ack",
            json={
                "success_keys": success_keys,
                "failed_keys": failed_keys,
                "failure_reason": failure_reason,
            },
        )
        resp.raise_for_status()


async def _execute_consumer(
    spec,
    records: list,
    tenant_id: str,
    feed_id: str,
    group_name: str,
) -> Tuple[List[str], List[str], Optional[str]]:
    """
    Invoke spec.handler inside an integration_context.

    Returns (success_keys, failed_keys, failure_reason).

    - batch=True  → single call with the full records list
    - batch=False → one call per record; exceptions are caught per-item
    """
    success_keys: List[str] = []
    failed_keys: List[str] = []
    failure_reason: Optional[str] = None

    first = records[0] if records else None
    with integration_context(
        tenant_id=tenant_id,
        integration=(
            first.source_integration if first and first.source_integration else feed_id
        ),
        integration_pipeline=(
            first.source_pipeline if first and first.source_pipeline else group_name
        ),
        parent_run_id=first.source_run_id if first else None,
        traceparent=first.source_traceparent if first else None,
    ):
        if spec.batch:
            try:
                await spec.handler(records)
                success_keys = [r.dedupe_key for r in records if r.dedupe_key]
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(
                    f"[consume-feed-batch] Batch handler failed for "
                    f"feed={feed_id} group={group_name}: {e}",
                    exc_info=True,
                )
                failed_keys = [r.dedupe_key for r in records if r.dedupe_key]
                failure_reason = str(e)
        else:
            for record in records:
                try:
                    await spec.handler(record)
                    if record.dedupe_key:
                        success_keys.append(record.dedupe_key)
                except Exception as e:
                    logger.error(
                        f"[consume-feed-batch] Handler failed for "
                        f"dedupe_key={record.dedupe_key}: {e}",
                        exc_info=True,
                    )
                    if record.dedupe_key:
                        failed_keys.append(record.dedupe_key)
                    if failure_reason is None:
                        failure_reason = str(e)

    return success_keys, failed_keys, failure_reason


def _flush() -> None:
    AsyncManager.get_instance().flush(15.0)


# ─── Command: consume-feed ───────────────────────────────────────────


async def cmd_consume_feed(encoded_envelope: str) -> None:
    """
    Decode base64 envelope → resolve consumer → build RecordData → execute handler.
    Flushes observability and calls sys.exit(0|1).
    """
    # 1. Decode envelope
    try:
        envelope = _decode_envelope(encoded_envelope)
    except ValueError as e:
        logger.error(f"[consume-feed] Invalid envelope: {e}")
        _flush()
        sys.exit(1)

    # 2. Resolve consumer
    try:
        spec = _resolve_feed_consumer(envelope.feed_id, envelope.group_name)
    except ValueError as e:
        logger.error(f"[consume-feed] {e}")
        _flush()
        sys.exit(1)

    # 3. Build RecordData
    item = {
        "dedupe_key": envelope.dedupe_key,
        "timestamp": envelope.timestamp,
        "data": envelope.data,
        "blob_ref": envelope.blob_ref,
        "record_type": envelope.record_type,
        "record_id": envelope.record_id,
        "source_integration": envelope.source_integration,
        "source_pipeline": envelope.source_pipeline,
        "source_run_id": envelope.source_run_id,
        "source_traceparent": envelope.source_traceparent,
    }
    record = await _resolve_record_from_item(item)
    if record is None:
        logger.error(
            f"[consume-feed] Could not resolve data for dedupe_key={envelope.dedupe_key}"
        )
        _flush()
        sys.exit(1)

    # 4. Execute handler
    success_keys, failed_keys, _ = await _execute_consumer(
        spec,
        [record],
        tenant_id=envelope.tenant_id,
        feed_id=envelope.feed_id,
        group_name=envelope.group_name,
    )

    _flush()
    sys.exit(0 if not failed_keys else 1)


# ─── Command: consume-feed-batch ────────────────────────────────────


async def cmd_consume_feed_batch(batch_id: str) -> None:
    """
    Fetch batch from API → resolve consumer → execute handler → ack results.
    Flushes observability and calls sys.exit(0|1).
    Exit 0 whenever the ack was sent successfully (even with failed_keys).
    Exit 1 only on a fatal error that prevented the ack from being sent.
    """
    api_url, api_key = _get_api_config()

    # 1. Fetch batch
    try:
        batch = await _fetch_batch(batch_id, api_url, api_key)
    except httpx.HTTPStatusError as e:
        logger.error(
            f"[consume-feed-batch] Failed to fetch batch {batch_id}: "
            f"HTTP {e.response.status_code} — {e.response.text}"
        )
        _flush()
        sys.exit(1)
    except Exception as e:
        logger.error(f"[consume-feed-batch] Failed to fetch batch {batch_id}: {e}")
        _flush()
        sys.exit(1)

    feed_id = batch.get("feed_id", "")
    group_name = batch.get("group_name", "")
    tenant_id = batch.get("tenant_id", "")
    items = batch.get("items", [])

    # 2. Resolve consumer
    success_keys: List[str] = []
    failed_keys: List[str] = []
    failure_reason: Optional[str] = None

    try:
        spec = _resolve_feed_consumer(feed_id, group_name)
    except ValueError as e:
        logger.error(f"[consume-feed-batch] {e}")
        failed_keys = [it["dedupe_key"] for it in items if it.get("dedupe_key")]
        failure_reason = str(e)
        try:
            await _ack_batch(
                batch_id, success_keys, failed_keys, failure_reason, api_url, api_key
            )
        except Exception as ack_e:
            logger.error(
                f"[consume-feed-batch] Ack failed after missing consumer: {ack_e}"
            )
            _flush()
            sys.exit(1)
        _flush()
        sys.exit(1)

    # 3. Resolve records (handles blob_ref)
    records_or_none = await asyncio.gather(
        *[_resolve_record_from_item(it) for it in items]
    )
    records = [r for r in records_or_none if r is not None]
    unresolvable = [
        it["dedupe_key"]
        for it, r in zip(items, records_or_none)
        if r is None and it.get("dedupe_key")
    ]
    if unresolvable:
        failed_keys.extend(unresolvable)
        failure_reason = "Blob resolution failed for some items"

    # 4. Execute handler
    if records:
        s_keys, f_keys, f_reason = await _execute_consumer(
            spec, records, tenant_id=tenant_id, feed_id=feed_id, group_name=group_name
        )
        success_keys.extend(s_keys)
        failed_keys.extend(f_keys)
        if f_reason and failure_reason is None:
            failure_reason = f_reason

    # 5. Ack results
    try:
        await _ack_batch(
            batch_id, success_keys, failed_keys, failure_reason, api_url, api_key
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            f"[consume-feed-batch] Ack failed for batch {batch_id}: "
            f"HTTP {e.response.status_code} — {e.response.text}"
        )
        _flush()
        sys.exit(1)
    except Exception as e:
        logger.error(f"[consume-feed-batch] Ack failed for batch {batch_id}: {e}")
        _flush()
        sys.exit(1)

    _flush()
    sys.exit(0)

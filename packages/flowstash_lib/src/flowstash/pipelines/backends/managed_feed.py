"""
Managed Feed Backend — HTTP proxy to the Integrator Platform API.

Drop-in replacement for RecordsFeed's Redis-based publishing when
running in the managed deployment mode. No Redis or GCP SDK dependency.
"""

import asyncio
import json
import logging
from datetime import datetime, UTC
from typing import Optional

import httpx

from flowstash.pipelines.records_model import RecordData
from flowstash.pipelines.record_serialization import to_jsonable
from flowstash.context import get_context
from flowstash.observability.ingestion import enqueue_record_link
from flowstash.observability.model import RecordLinkKind

logger = logging.getLogger(__name__)

_BLOB_OFFLOAD_THRESHOLD_BYTES = 1024 * 1024  # 1 MB

# Retry config for transient publish failures
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE_SEC = 1.0  # 1s → 2s → 4s


class ManagedFeedBackend:
    """
    Feed backend that publishes records via the Integrator Platform API.

    The platform API handles deduplication using Firestore transactions.
    """

    def __init__(
        self,
        api_url: str,
        auth_token: str,
        project_id: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        """
        Args:
            api_url: Base URL of the platform API.
            auth_token: JWT token for authentication.
            project_id: Logical project identifier (injected as MANAGED_PROJECT_ID at deploy time).
            environment: Deployment environment, e.g. "prod" or "dev"
              (injected as ENVIRONMENT at deploy time).
        """
        import os

        self.api_url = api_url.rstrip("/")
        self.project_id = (
            project_id
            or os.environ.get("MANAGED_PROJECT_ID")
            or os.environ.get("FLOWSTASH_PROJECT_ID")
        )
        self.environment = environment or os.environ.get("ENVIRONMENT")

        if not self.project_id or not self.environment:
            raise ValueError(
                "ManagedFeedBackend requires project_id and environment. "
                "Ensure MANAGED_PROJECT_ID and ENVIRONMENT are set in the environment variables."
            )
        self._client = httpx.AsyncClient(
            base_url=self.api_url,
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def publish(self, feed_id: str, record: RecordData) -> str:
        """
        Publish a record to a feed via the Platform API.

        Large payloads (>5 KB) are offloaded to BlobStore — the reference is
        forwarded to the API rather than the raw data, matching the Redis path.

        Transient HTTP errors (5xx, connection errors, timeouts) are retried
        up to {_MAX_RETRIES} times with exponential backoff.

        Args:
            feed_id: The feed identifier.
            record: The record to publish.

        Returns:
            "published" if the record was written, "deduped" if skipped.
        """
        ctx = get_context()
        integration = ctx.integration if ctx else "unknown"
        dedupe_key = record.get_dedupe_key(integration)

        # Use record timestamp or now
        effective_ts = record.timestamp or datetime.now(UTC)
        ts_val = effective_ts.timestamp()

        # BlobStore offloading for large payloads (>5 KB), matching Redis path
        data_to_send = to_jsonable(record.data)
        blob_ref: Optional[str] = None

        try:
            raw_data = json.dumps(data_to_send).encode("utf-8")
            if len(raw_data) > _BLOB_OFFLOAD_THRESHOLD_BYTES:
                from flowstash.observability.registry import get_blob_store

                blob_path = (
                    f"feeds/{feed_id}/{record.record_type or 'record'}/{dedupe_key}"
                )
                try:
                    store = get_blob_store()

                    # Validation: Managed backend requires a remote/shared blob store for > 1MB records.
                    # We check against known local/noop stores.
                    from flowstash.observability.registry import (
                        NoOpBlobStore,
                        ConsoleBlobStore,
                        FileStore,
                    )

                    if store.__class__.__name__ in (
                        "NoOpBlobStore",
                        "ConsoleBlobStore",
                        "FileStore",
                    ):
                        raise ValueError(
                            f"Record payload ({len(raw_data)} bytes) exceeds the 1MB limit for Managed Backend. "
                            "A shared remote BlobStore (e.g., Google Cloud Storage) must be configured in "
                            "your observability settings to offload large records. "
                            "Local file-based stores are not supported as the API and Worker run in isolated environments."
                        )

                    blob_ref, _, _ = store.put(
                        path_hint=blob_path,
                        content_type="application/json",
                        data=raw_data,
                    )
                    data_to_send = None  # Only store the reference
                except ValueError:
                    # Re-raise validation errors (e.g., store type check for large payloads)
                    raise
                except Exception as blob_err:
                    logger.warning(
                        f"BlobStore offload failed for {dedupe_key}, "
                        f"falling back to inline data: {blob_err}"
                    )
        except ValueError:
            raise
        except Exception as serial_err:
            logger.warning(
                f"Could not serialize record data for size check: {serial_err}"
            )

        payload = {
            "dedupe_key": dedupe_key,
            "timestamp": ts_val,
            "data": data_to_send,
            "blob_ref": blob_ref,
            "record_type": record.record_type,
            "record_id": record.record_id,
            "project_id": self.project_id,
            "environment": self.environment,
            "source_integration": ctx.integration if ctx else None,
            "source_pipeline": ctx.integration_pipeline if ctx else None,
            "source_run_id": ctx.run_id if ctx else None,
            "source_traceparent": ctx.traceparent if ctx else None,
        }

        # Retry loop with exponential backoff for transient errors (F4)
        last_exc: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = await self._client.post(
                    f"/v1/feed/{feed_id}/publish",
                    json=payload,
                )
                # 4xx = client error — don't retry (structural problem)
                if 400 <= response.status_code < 500:
                    response.raise_for_status()

                # 5xx = server/transient error — retry
                if response.status_code >= 500:
                    err_text = response.text[:200]
                    last_exc = httpx.HTTPStatusError(
                        message=f"Server error {response.status_code}: {err_text}",
                        request=response.request,
                        response=response,
                    )
                    if attempt < _MAX_RETRIES:
                        delay = _RETRY_BACKOFF_BASE_SEC * (2**attempt)
                        logger.warning(
                            f"Publish transient error (attempt {attempt + 1}/{_MAX_RETRIES + 1}) "
                            f"for feed={feed_id} key={dedupe_key}: {response.status_code}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        await asyncio.sleep(delay)
                        continue
                    raise last_exc

                result = response.json()
                status = result.get("status", "unknown")
                break  # success — exit retry loop

            except (
                httpx.ConnectError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
            ) as e:
                last_exc = e
                if attempt < _MAX_RETRIES:
                    delay = _RETRY_BACKOFF_BASE_SEC * (2**attempt)
                    logger.warning(
                        f"Publish connection error (attempt {attempt + 1}/{_MAX_RETRIES + 1}) "
                        f"for feed={feed_id} key={dedupe_key}: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Publish failed after {_MAX_RETRIES + 1} attempts "
                        f"for feed={feed_id} key={dedupe_key}: {e}"
                    )
                    raise
        else:
            # All retries exhausted via the 5xx path
            raise last_exc  # type: ignore[misc]

        # Emit observability event
        if ctx:
            await enqueue_record_link(
                tenant_id=ctx.tenant_id,
                integration=integration,
                pipeline=ctx.integration_pipeline,
                run_id=ctx.run_id,
                trace_id=ctx.trace_id,
                span_id=ctx.span_id,
                record_key=dedupe_key,
                kind=RecordLinkKind.PUBLISHED,
                source=feed_id,
            )

        return status

    async def close(self):
        """Close the HTTP client — must be called on worker shutdown."""
        await self._client.aclose()

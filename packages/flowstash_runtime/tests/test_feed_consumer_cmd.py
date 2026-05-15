"""
Tests for feed_consumer.py command handlers and helpers.
"""

import asyncio
import base64
import json
import sys
from dataclasses import dataclass
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ─── Fixtures / helpers ──────────────────────────────────────────────


def _make_envelope(**overrides) -> dict:
    base = {
        "tenant_id": "tenant-1",
        "project_id": "proj-1",
        "environment": "test",
        "feed_id": "my-feed",
        "group_name": "my-group",
        "dedupe_key": "key-123",
        "timestamp": 1700000000.0,
        "data": {"value": 42},
        "blob_ref": None,
        "record_type": "order",
        "record_id": "order-1",
    }
    base.update(overrides)
    return base


def _b64_encode(obj: dict) -> str:
    return base64.b64encode(json.dumps(obj).encode()).decode()


def _make_spec(feed_id="my-feed", group_name="my-group", batch=False, handler=None):
    from flowstash.pipelines.consumer import ConsumerSpec

    if handler is None:
        handler = AsyncMock()
    return ConsumerSpec(
        handler=handler,
        feed_id=feed_id,
        subscription_name=group_name,
        batch=batch,
        max_batch_size=100,
        max_delay_ms=500,
        rate_limit_per_sec=None,
        concurrency=None,
    )


# ─── _decode_envelope ────────────────────────────────────────────────


def test_decode_envelope_valid():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        _decode_envelope,
        ClassicFeedEnvelope,
    )

    raw = _make_envelope()
    encoded = _b64_encode(raw)
    envelope = _decode_envelope(encoded)

    assert isinstance(envelope, ClassicFeedEnvelope)
    assert envelope.feed_id == "my-feed"
    assert envelope.group_name == "my-group"
    assert envelope.dedupe_key == "key-123"
    assert envelope.data == {"value": 42}


def test_decode_envelope_bad_base64():
    from flowstash.runtime.worker.backends.managed.feed_consumer import _decode_envelope

    with pytest.raises(ValueError, match="Bad base64"):
        _decode_envelope("!!!not-base64!!!")


def test_decode_envelope_bad_json():
    from flowstash.runtime.worker.backends.managed.feed_consumer import _decode_envelope

    encoded = base64.b64encode(b"not-json").decode()
    with pytest.raises(ValueError, match="Bad JSON"):
        _decode_envelope(encoded)


def test_decode_envelope_missing_required_field():
    from flowstash.runtime.worker.backends.managed.feed_consumer import _decode_envelope

    raw = _make_envelope()
    del raw["dedupe_key"]
    with pytest.raises(ValueError, match="missing required fields"):
        _decode_envelope(_b64_encode(raw))


# ─── _resolve_feed_consumer ──────────────────────────────────────────


def test_resolve_feed_consumer_found():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        _resolve_feed_consumer,
    )

    spec = _make_spec("my-feed", "my-group")
    with patch(
        "flowstash.pipelines.consumer._consumers",
        [spec],
    ):
        result = _resolve_feed_consumer("my-feed", "my-group")
        assert result is spec


def test_resolve_feed_consumer_not_found():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        _resolve_feed_consumer,
    )

    spec = _make_spec("other-feed", "other-group")
    with patch(
        "flowstash.pipelines.consumer._consumers",
        [spec],
    ):
        with pytest.raises(ValueError, match="No @feed_consumer"):
            _resolve_feed_consumer("my-feed", "my-group")


def test_resolve_feed_consumer_wrong_feed_id():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        _resolve_feed_consumer,
    )

    # Same group_name but different feed_id — must not match
    spec = _make_spec("other-feed", "my-group")
    with patch(
        "flowstash.pipelines.consumer._consumers",
        [spec],
    ):
        with pytest.raises(ValueError):
            _resolve_feed_consumer("my-feed", "my-group")


# ─── cmd_consume_feed ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cmd_consume_feed_success():
    from flowstash.runtime.worker.backends.managed.feed_consumer import cmd_consume_feed

    handler = AsyncMock()
    spec = _make_spec(handler=handler)
    raw = _make_envelope()
    encoded = _b64_encode(raw)

    with (
        patch("flowstash.pipelines.consumer._consumers", [spec]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._flush",
        ),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.integration_context",
        ) as mock_ctx,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        await cmd_consume_feed(encoded)

    assert exc_info.value.code == 0
    handler.assert_called_once()


@pytest.mark.asyncio
async def test_cmd_consume_feed_handler_raises():
    from flowstash.runtime.worker.backends.managed.feed_consumer import cmd_consume_feed

    handler = AsyncMock(side_effect=RuntimeError("boom"))
    spec = _make_spec(handler=handler)
    raw = _make_envelope()
    encoded = _b64_encode(raw)

    with (
        patch("flowstash.pipelines.consumer._consumers", [spec]),
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.integration_context",
        ) as mock_ctx,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        await cmd_consume_feed(encoded)

    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_cmd_consume_feed_bad_envelope():
    from flowstash.runtime.worker.backends.managed.feed_consumer import cmd_consume_feed

    with (
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await cmd_consume_feed("!!!bad!!!")

    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_cmd_consume_feed_no_consumer():
    from flowstash.runtime.worker.backends.managed.feed_consumer import cmd_consume_feed

    raw = _make_envelope()
    encoded = _b64_encode(raw)

    with (
        patch("flowstash.pipelines.consumer._consumers", []),
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await cmd_consume_feed(encoded)

    assert exc_info.value.code == 1


# ─── cmd_consume_feed_batch ──────────────────────────────────────────


def _make_batch_response(feed_id="my-feed", group_name="my-group", n_items=2):
    return {
        "run_id": "run-abc",
        "feed_id": feed_id,
        "group_name": group_name,
        "tenant_id": "tenant-1",
        "items": [
            {
                "dedupe_key": f"key-{i}",
                "timestamp": 1700000000.0 + i,
                "data": {"idx": i},
                "record_type": "order",
                "record_id": f"order-{i}",
            }
            for i in range(n_items)
        ],
    }


@pytest.mark.asyncio
async def test_cmd_consume_feed_batch_success():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        cmd_consume_feed_batch,
    )

    handler = AsyncMock()
    spec = _make_spec(handler=handler)
    batch = _make_batch_response()

    with (
        patch("flowstash.pipelines.consumer._consumers", [spec]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._fetch_batch",
            new=AsyncMock(return_value=batch),
        ),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._ack_batch",
            new=AsyncMock(),
        ) as mock_ack,
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.integration_context",
        ) as mock_ctx,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        await cmd_consume_feed_batch("batch-123")

    assert exc_info.value.code == 0
    assert handler.call_count == 2  # two items, non-batch spec
    # Ack must have been called with success keys
    ack_kwargs = mock_ack.call_args
    assert ack_kwargs is not None


@pytest.mark.asyncio
async def test_cmd_consume_feed_batch_success_batch_mode():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        cmd_consume_feed_batch,
    )

    handler = AsyncMock()
    spec = _make_spec(handler=handler, batch=True)
    batch = _make_batch_response()

    with (
        patch("flowstash.pipelines.consumer._consumers", [spec]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._fetch_batch",
            new=AsyncMock(return_value=batch),
        ),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._ack_batch",
            new=AsyncMock(),
        ),
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.integration_context",
        ) as mock_ctx,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        await cmd_consume_feed_batch("batch-123")

    assert exc_info.value.code == 0
    handler.assert_called_once()  # batch=True → single call with list


@pytest.mark.asyncio
async def test_cmd_consume_feed_batch_fetch_fails():
    import httpx
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        cmd_consume_feed_batch,
    )

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"

    with (
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._fetch_batch",
            new=AsyncMock(
                side_effect=httpx.HTTPStatusError(
                    "404", request=MagicMock(), response=mock_response
                )
            ),
        ),
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await cmd_consume_feed_batch("batch-999")

    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_cmd_consume_feed_batch_ack_fails():
    import httpx
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        cmd_consume_feed_batch,
    )

    handler = AsyncMock()
    spec = _make_spec(handler=handler)
    batch = _make_batch_response()

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Error"

    with (
        patch("flowstash.pipelines.consumer._consumers", [spec]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._fetch_batch",
            new=AsyncMock(return_value=batch),
        ),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._ack_batch",
            new=AsyncMock(
                side_effect=httpx.HTTPStatusError(
                    "500", request=MagicMock(), response=mock_response
                )
            ),
        ),
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.integration_context",
        ) as mock_ctx,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        await cmd_consume_feed_batch("batch-123")

    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_cmd_consume_feed_batch_no_handler():
    from flowstash.runtime.worker.backends.managed.feed_consumer import (
        cmd_consume_feed_batch,
    )

    batch = _make_batch_response()

    with (
        patch("flowstash.pipelines.consumer._consumers", []),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._fetch_batch",
            new=AsyncMock(return_value=batch),
        ),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer._ack_batch",
            new=AsyncMock(),
        ) as mock_ack,
        patch("flowstash.runtime.worker.backends.managed.feed_consumer._flush"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await cmd_consume_feed_batch("batch-123")

    # exits 1 (consumer not found) but ack was still called with all items as failed
    assert exc_info.value.code == 1
    ack_call = mock_ack.call_args
    assert ack_call is not None
    _, kwargs_or_args = ack_call
    # Either positional or keyword — check the failed_keys argument (3rd positional)
    all_args = ack_call.args
    failed = all_args[2] if len(all_args) > 2 else ack_call.kwargs.get("failed_keys", [])
    assert set(failed) == {"key-0", "key-1"}


# ─── ManagedConsumer.start() dispatch ────────────────────────────────


@pytest.mark.asyncio
async def test_managed_consumer_dispatch_consume_feed():
    """ManagedConsumer.start() with 'consume-feed' dispatches to cmd_consume_feed."""
    from flowstash.config.runtime_config import RuntimeConfig
    from flowstash.runtime.worker.backends.managed.managed_consumer import (
        ManagedConsumer,
    )

    config = RuntimeConfig()
    consumer = ManagedConsumer(config)
    envelope_b64 = _b64_encode(_make_envelope())

    called_with = []

    async def fake_cmd_consume_feed(encoded):
        called_with.append(encoded)
        sys.exit(0)

    with (
        patch("sys.argv", ["worker_main.py", "consume-feed", envelope_b64]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.cmd_consume_feed",
            side_effect=fake_cmd_consume_feed,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        await consumer.start()

    assert exc_info.value.code == 0
    assert called_with == [envelope_b64]


@pytest.mark.asyncio
async def test_managed_consumer_dispatch_consume_feed_batch():
    """ManagedConsumer.start() with 'consume-feed-batch' dispatches to cmd_consume_feed_batch."""
    from flowstash.config.runtime_config import RuntimeConfig
    from flowstash.runtime.worker.backends.managed.managed_consumer import (
        ManagedConsumer,
    )

    config = RuntimeConfig()
    consumer = ManagedConsumer(config)

    called_with = []

    async def fake_cmd_consume_feed_batch(batch_id):
        called_with.append(batch_id)
        sys.exit(0)

    with (
        patch("sys.argv", ["worker_main.py", "consume-feed-batch", "batch-abc"]),
        patch(
            "flowstash.runtime.worker.backends.managed.feed_consumer.cmd_consume_feed_batch",
            side_effect=fake_cmd_consume_feed_batch,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        await consumer.start()

    assert exc_info.value.code == 0
    assert called_with == ["batch-abc"]


@pytest.mark.asyncio
async def test_managed_consumer_unknown_command_exits_1():
    from flowstash.config.runtime_config import RuntimeConfig
    from flowstash.runtime.worker.backends.managed.managed_consumer import (
        ManagedConsumer,
    )

    config = RuntimeConfig()
    consumer = ManagedConsumer(config)

    with (
        patch("sys.argv", ["worker_main.py", "not-a-command"]),
        pytest.raises(SystemExit) as exc_info,
    ):
        await consumer.start()

    assert exc_info.value.code == 1

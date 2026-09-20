"""
Tests for scoped feed-consumer snapshot sync.

Covers:
1. Payload groups local consumers by feed_id.
2. Empty local registry sends {"feeds": []}.
3. register_schedules syncs feed consumers before the no-tasks early exit.
4. Startup sync is called from the managed HTTP worker lifespan hook.
5. The snapshot request targets /v1/feed/consumers/sync, not the old per-feed URL.
"""

import sys
from unittest.mock import MagicMock, patch, call

import httpx
import pytest

import flowstash.runtime.worker.backends.managed.managed_consumer as mc
from flowstash.runtime.worker.backends.managed.managed_consumer import (
    _build_feed_consumers_payload,
    _resolve_managed_api_context,
    sync_feed_consumers,
)
from flowstash.pipelines.consumer import ConsumerSpec

# ── helpers ──────────────────────────────────────────────────────────


def _make_spec(feed_id: str, subscription_name: str, **kwargs) -> ConsumerSpec:
    return ConsumerSpec(
        handler=lambda r: None,
        feed_id=feed_id,
        subscription_name=subscription_name,
        batch=kwargs.get("batch", False),
        max_batch_size=kwargs.get("max_batch_size", 100),
        max_delay_ms=kwargs.get("max_delay_ms", 500),
        rate_limit_per_sec=None,
        concurrency=None,
        dedupe_window_ms=kwargs.get("dedupe_window_ms"),
    )


# ── 1. Payload groups by feed_id ─────────────────────────────────────


def test_build_payload_groups_by_feed_id():
    specs = [
        _make_spec(
            "orders", "warehouse_a", batch=False, max_batch_size=100, max_delay_ms=500
        ),
        _make_spec(
            "orders", "warehouse_b", batch=True, max_batch_size=50, max_delay_ms=250
        ),
        _make_spec(
            "shipments", "logistics", batch=False, max_batch_size=200, max_delay_ms=1000
        ),
    ]

    with patch.object(mc, "get_registered_consumers", return_value=specs):
        payload = _build_feed_consumers_payload()

    assert set(f["feed_id"] for f in payload["feeds"]) == {"orders", "shipments"}

    orders_feed = next(f for f in payload["feeds"] if f["feed_id"] == "orders")
    assert len(orders_feed["consumers"]) == 2
    group_names = {c["group_name"] for c in orders_feed["consumers"]}
    assert group_names == {"warehouse_a", "warehouse_b"}

    wa = next(c for c in orders_feed["consumers"] if c["group_name"] == "warehouse_a")
    assert wa == {
        "group_name": "warehouse_a",
        "batch": False,
        "max_batch_size": 100,
        "max_delay_ms": 500,
        "debounce_delay_ms": 0,
        "max_debounce_window_ms": 0,
        "dedupe_window_ms": None,
    }

    wb = next(c for c in orders_feed["consumers"] if c["group_name"] == "warehouse_b")
    assert wb == {
        "group_name": "warehouse_b",
        "batch": True,
        "max_batch_size": 50,
        "max_delay_ms": 250,
        "debounce_delay_ms": 0,
        "max_debounce_window_ms": 0,
        "dedupe_window_ms": None,
    }

    shipments_feed = next(f for f in payload["feeds"] if f["feed_id"] == "shipments")
    assert len(shipments_feed["consumers"]) == 1


def test_dedupe_window_key_is_always_emitted():
    """The key must be present even when unset, as an explicit null.

    The platform reads an *absent* key as "this client is too old to know about
    dedupe suppression" and disables it permanently. Dropping the key here (for
    example by omitting None values) would silently opt every consumer out.
    """
    specs = [_make_spec("orders", "warehouse_a")]

    with patch.object(mc, "get_registered_consumers", return_value=specs):
        payload = _build_feed_consumers_payload()

    consumer = payload["feeds"][0]["consumers"][0]
    assert "dedupe_window_ms" in consumer
    assert consumer["dedupe_window_ms"] is None


def test_declared_dedupe_window_is_forwarded():
    specs = [_make_spec("orders", "warehouse_a", dedupe_window_ms=60000)]

    with patch.object(mc, "get_registered_consumers", return_value=specs):
        payload = _build_feed_consumers_payload()

    assert payload["feeds"][0]["consumers"][0]["dedupe_window_ms"] == 60000


# ── 2. Empty registry sends {"feeds": []} ────────────────────────────


def test_build_payload_empty_registry():
    with patch.object(mc, "get_registered_consumers", return_value=[]):
        payload = _build_feed_consumers_payload()

    assert payload == {"feeds": []}


def test_sync_feed_consumers_sends_empty_payload(monkeypatch):
    """Even with no consumers the POST must be sent with feeds=[]."""
    sent = []

    def fake_post(url, json):
        sent.append((url, json))
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setenv("FLOWSTASH_API_URL", "https://api.example.com")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")

    with patch.object(mc, "get_registered_consumers", return_value=[]):
        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.post = fake_post
            mock_client_cls.return_value = mock_client

            result = sync_feed_consumers(strict=True)

    assert result is True
    assert len(sent) == 1
    url, body = sent[0]
    assert "/v1/feed/consumers/sync" in url
    assert body == {"feeds": []}


# ── 3. register_schedules syncs consumers before no-tasks early exit ──


def test_register_schedules_syncs_before_no_tasks_exit(monkeypatch):
    """
    When there are no scheduled tasks, register_schedules should still call
    _cmd_sync_feed_consumers and then sys.exit(0).
    """
    monkeypatch.setenv("FLOWSTASH_API_URL", "https://api.example.com")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")

    sync_calls = []

    def fake_sync(api_url, auth_token):
        sync_calls.append((api_url, auth_token))

    fake_backend = MagicMock()
    fake_backend._registered_tasks = []

    with patch.object(mc, "_cmd_sync_feed_consumers", fake_sync):
        with patch("flowstash.queue.backend.get_backend", return_value=fake_backend):
            with pytest.raises(SystemExit) as exc_info:
                mc._cmd_register_schedules(deploy_id="deploy-123")

    assert exc_info.value.code == 0
    assert (
        len(sync_calls) == 1
    ), "Consumer sync must be called even with no scheduled tasks"


def test_register_schedules_syncs_before_task_registration(monkeypatch):
    """Consumer sync must happen before the task registration POST."""
    monkeypatch.setenv("FLOWSTASH_API_URL", "https://api.example.com")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")

    call_order = []

    def fake_sync(api_url, auth_token):
        call_order.append("sync")

    fake_backend = MagicMock()
    fake_backend._registered_tasks = [
        {
            "task_id": "t1",
            "task_name": "my_task",
            "integration": "x",
            "pipeline": "y",
            "default_schedule": None,
        }
    ]

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    def fake_post(url, json):
        call_order.append("tasks_post")
        return mock_response

    with patch.object(mc, "_cmd_sync_feed_consumers", fake_sync):
        with patch("flowstash.queue.backend.get_backend", return_value=fake_backend):
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client.__enter__ = MagicMock(return_value=mock_client)
                mock_client.__exit__ = MagicMock(return_value=False)
                mock_client.post = fake_post
                mock_client_cls.return_value = mock_client

                with pytest.raises(SystemExit) as exc_info:
                    mc._cmd_register_schedules(deploy_id="deploy-456")

    assert exc_info.value.code == 0
    assert call_order == [
        "sync",
        "tasks_post",
    ], "Sync must happen before task registration"


# ── 4. Startup sync is called from the managed HTTP worker lifespan ───


@pytest.mark.asyncio
async def test_lifespan_calls_sync_feed_consumers_on_startup(monkeypatch):
    """The FastAPI lifespan must call asyncio.to_thread(sync_feed_consumers, strict=False)."""
    import asyncio
    from flowstash.config.runtime_config import RuntimeConfig
    import flowstash.runtime.worker.backends.managed.main as main_mod
    from fastapi import FastAPI

    to_thread_calls = []

    async def fake_to_thread(fn, *args, **kwargs):
        to_thread_calls.append({"fn": fn, "args": args, "kwargs": kwargs})
        return fn(*args, **kwargs)

    async def fake_shutdown(*a, **kw):
        pass

    with patch.object(main_mod, "asyncio") as mock_asyncio:
        mock_asyncio.to_thread = fake_to_thread

        with patch.object(main_mod, "initialize_runtime"):
            with patch.object(main_mod, "_shutdown_managed_runtime", fake_shutdown):
                with patch(
                    "flowstash.queue.backend.get_backend",
                    side_effect=RuntimeError("not init"),
                ):
                    lifespan_factory = main_mod._build_managed_lifespan(RuntimeConfig())
                    app = FastAPI()
                    app.state.managed_task_drain_controller = MagicMock()

                    async with lifespan_factory(app):
                        pass  # startup complete

    sync_calls = [
        c
        for c in to_thread_calls
        if getattr(c["fn"], "__name__", "") == "sync_feed_consumers"
    ]
    assert (
        len(sync_calls) >= 1
    ), "asyncio.to_thread(sync_feed_consumers, ...) must be called during startup"
    assert (
        sync_calls[0]["kwargs"].get("strict") is False
    ), "Startup sync must use strict=False (best-effort)"


# ── 5. Snapshot request targets /v1/feed/consumers/sync ──────────────


def test_sync_url_is_scoped_endpoint(monkeypatch):
    """
    sync_feed_consumers must POST to /v1/feed/consumers/sync, not the old
    per-feed /v1/feed/{feed_id}/consumers/register path.
    """
    monkeypatch.setenv("FLOWSTASH_API_URL", "https://api.example.com")
    monkeypatch.setenv("MANAGED_AUTH_TOKEN", "tok")

    specs = [_make_spec("orders", "warehouse_a")]
    posted_urls = []

    def fake_post(url, json):
        posted_urls.append(url)
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    with patch.object(mc, "get_registered_consumers", return_value=specs):
        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.post = fake_post
            mock_client_cls.return_value = mock_client

            sync_feed_consumers(strict=True)

    assert len(posted_urls) == 1
    assert posted_urls[0].endswith(
        "/v1/feed/consumers/sync"
    ), f"Expected /v1/feed/consumers/sync, got {posted_urls[0]!r}"
    for url in posted_urls:
        assert (
            "/v1/feed/orders/consumers/register" not in url
        ), "Old per-feed registration URL must not be used"

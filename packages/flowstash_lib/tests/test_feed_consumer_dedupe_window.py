"""Tests for the dedupe_window_ms option on the feed_consumer decorator."""

import pytest

from flowstash.pipelines.consumer import (
    ConsumerSpec,
    feed_consumer,
    get_registered_consumers,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """The consumer registry is module-level; keep tests from leaking into each other."""
    registry = get_registered_consumers()
    before = list(registry)
    yield
    registry.clear()
    registry.extend(before)


def test_default_is_none_meaning_no_opinion():
    """None, not 0 — the platform default must still be reachable."""
    assert ConsumerSpec.__dataclass_fields__["dedupe_window_ms"].default is None


def test_declared_window_is_registered():
    @feed_consumer("orders", subscription="grp_a", dedupe_window_ms=60000)
    def handler(records):
        return None

    spec = next(
        s for s in get_registered_consumers() if s.subscription_name == "grp_a"
    )
    assert spec.dedupe_window_ms == 60000


def test_explicit_zero_opts_out():
    @feed_consumer("orders", subscription="grp_b", dedupe_window_ms=0)
    def handler(records):
        return None

    spec = next(
        s for s in get_registered_consumers() if s.subscription_name == "grp_b"
    )
    assert spec.dedupe_window_ms == 0


def test_rejects_batched_consumer():
    """The batched buffer already collapses same-key records latest-wins."""
    with pytest.raises(ValueError, match="classic"):

        @feed_consumer("orders", subscription="grp_c", batch=True, dedupe_window_ms=1000)
        def handler(records):
            return None


def test_rejects_combination_with_debounce():
    with pytest.raises(ValueError, match="debounce"):

        @feed_consumer(
            "orders",
            subscription="grp_d",
            debounce_delay_ms=1000,
            dedupe_window_ms=1000,
        )
        def handler(records):
            return None

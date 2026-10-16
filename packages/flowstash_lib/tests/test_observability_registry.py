from unittest.mock import MagicMock

import pytest

from flowstash.config.observability_config import ObservabilityConfig, StoreType
from flowstash.observability import registry
from flowstash.observability.stores import api_stores


@pytest.fixture
def fake_api_stores(monkeypatch):
    """Replace managed API stores with mocks so configure() spawns no threads/clients."""
    mocks = {}
    for name in ("ApiEventsStore", "ApiDataExchangeStore", "ApiRecordsStore", "ApiBlobStore"):
        mocks[name] = MagicMock(name=name)
        monkeypatch.setattr(api_stores, name, mocks[name])
    yield mocks
    registry.configure(ObservabilityConfig(store_type=StoreType.DISABLED))


def _managed_config(**kwargs) -> ObservabilityConfig:
    return ObservabilityConfig(store_type=StoreType.MANAGED, project_id="proj-1", **kwargs)


def test_managed_api_key_falls_back_to_env(monkeypatch, fake_api_stores):
    monkeypatch.setenv("FLOWSTASH_API_KEY", "env-key")

    registry.configure(_managed_config())

    args = fake_api_stores["ApiEventsStore"].call_args.args
    assert args[1] == "env-key"
    assert fake_api_stores["ApiBlobStore"].call_args.args[1] == "env-key"


def test_managed_api_key_config_wins_over_env(monkeypatch, fake_api_stores):
    monkeypatch.setenv("FLOWSTASH_API_KEY", "env-key")

    registry.configure(_managed_config(managed_api_key="config-key"))

    assert fake_api_stores["ApiEventsStore"].call_args.args[1] == "config-key"


def test_managed_api_key_empty_config_falls_back_to_env(monkeypatch, fake_api_stores):
    # `managedApiKey: ${FLOWSTASH_API_KEY}` can resolve to "" when the var was unset at load time.
    monkeypatch.setenv("FLOWSTASH_API_KEY", "env-key")

    registry.configure(_managed_config(managed_api_key=""))

    assert fake_api_stores["ApiEventsStore"].call_args.args[1] == "env-key"


def test_managed_api_key_missing_raises_clear_error(monkeypatch, fake_api_stores):
    monkeypatch.delenv("FLOWSTASH_API_KEY", raising=False)

    with pytest.raises(ValueError, match="FLOWSTASH_API_KEY"):
        registry.configure(_managed_config())

    fake_api_stores["ApiEventsStore"].assert_not_called()

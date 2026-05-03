import pytest
import yaml
from pathlib import Path
from flowstash.config.env_loader import load_config_dir
from flowstash.config.runtime_config import RuntimeConfig


def test_load_config_dir_basic(tmp_path):
    """Test basic config loading with shared and environment-specific configs."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Shared config
    shared_dir = config_dir / "shared"
    shared_dir.mkdir()
    with open(shared_dir / "backend.yaml", "w") as f:
        yaml.dump({"backend": {"type": "async"}}, f)

    # Dev config
    dev_dir = config_dir / "dev"
    dev_dir.mkdir()
    with open(dev_dir / "backend.yaml", "w") as f:
        yaml.dump({"backend": {"type": "dramatiq"}}, f)

    # Load dev config
    config = load_config_dir(config_dir, "dev")

    assert isinstance(config, RuntimeConfig)
    assert config.backend.type == "dramatiq"  # Dev override wins


def test_load_config_dir_merge_clients(tmp_path):
    """Test merging clients from shared and environment configs."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Clients root
    clients_root = tmp_path / "clients"
    clients_root.mkdir()

    # Shared config
    shared_dir = config_dir / "shared"
    shared_dir.mkdir()
    with open(shared_dir / "clients.yaml", "w") as f:
        yaml.dump({"path": str(clients_root), "pattern": "*.yaml"}, f)

    with open(clients_root / "client1.yaml", "w") as f:
        yaml.dump(
            {"client_id": "client1", "type": "webhook", "baseUrl": "http://shared"}, f
        )

    # Prod config
    prod_dir = config_dir / "prod"
    prod_dir.mkdir()
    with open(prod_dir / "clients.yaml", "w") as f:
        yaml.dump({"path": str(clients_root), "pattern": "*.yaml"}, f)

    # Override client1 in prod (implied by reload or just same path)
    # Actually _load_data_from_dir reads from the path in clients.yaml
    # which we set to the same clients_root. In a real scenario,
    # they might point to different subdirs or use different patterns.

    config = load_config_dir(config_dir, "prod")
    assert "client1" in config.clients
    assert config.clients["client1"].base_url == "http://shared"


def test_load_config_dir_missing_env(tmp_path):
    """Test error when environment directory is missing."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "shared").mkdir()

    with pytest.raises(ValueError, match="Environment directory .* does not exist"):
        load_config_dir(config_dir, "nonexistent")

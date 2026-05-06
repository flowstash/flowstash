import os
import re
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
from .runtime_config import (
    RuntimeConfig,
    ClientsConfigPointer,
    BackendConfig,
    WebhooksConfig,
    StateStoreConfig,
)
from .observability_config import ObservabilityConfig
from flowstash.clients.config import ClientSettings
from flowstash.clients.registry import init_registry


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge *override* into *base*. Dict values are merged; all other types are replaced."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def load_env_yaml(path: str | Path) -> Dict[str, Any]:
    """Load an environment YAML file with environment variable substitution."""
    with open(path, "r") as f:
        content = f.read()

        # Replace ${VAR} with environment variable values
        # We use a non-greedy match for the variable name
        content = re.sub(r"\$\{([^}]+)\}", lambda m: os.getenv(m.group(1), ""), content)

        data = yaml.safe_load(content)
        return data if isinstance(data, dict) else {}


def _load_data_from_dir(
    path: Path,
) -> tuple[
    Optional[Dict[str, Any]], Optional[Dict[str, Any]], Dict[str, ClientSettings]
]:
    """Helper to load config components from a single directory."""
    # 1. Observability
    obs_file = path / "observability.yaml"
    observability = None
    if obs_file.exists():
        observability = load_env_yaml(obs_file)

    # 2. Backend
    backend_file = path / "backend.yaml"
    backend = None
    if backend_file.exists():
        backend = load_env_yaml(backend_file)

    # 3. Clients
    clients_file = path / "clients.yaml"
    clients: Dict[str, ClientSettings] = {}

    if clients_file.exists():
        clients_pointer_data = load_env_yaml(clients_file)
        pointer = ClientsConfigPointer.model_validate(clients_pointer_data)

        clients_dir = Path(pointer.path)
        if not clients_dir.is_absolute():
            clients_dir = (path / clients_dir).resolve()

        if not clients_dir.is_dir():
            raise ValueError(
                f"Clients directory {clients_dir} (from clients.yaml) does not exist"
            )

        pattern = pointer.pattern
        recursive = pointer.recursive

        search_pattern = f"**/{pattern}" if recursive else pattern
        client_files = list(clients_dir.glob(search_pattern))

        for cf in client_files:
            if not cf.is_file():
                continue

            client_data = load_env_yaml(cf)
            client_id = (
                client_data.get("client_id") or client_data.get("clientId") or cf.stem
            )

            if client_id in clients:
                raise ValueError(f"Duplicate client_id '{client_id}' found in {cf}")

            if "client_id" not in client_data:
                client_data["client_id"] = client_id

            settings = ClientSettings.model_validate(client_data)

            # Known keys to exclude from extra
            settings_fields = set(ClientSettings.model_fields.keys())
            for field in ClientSettings.model_fields.values():
                if field.alias:
                    settings_fields.add(field.alias)

            known_keys = settings_fields | {"client_id", "clientId"}
            extra = {k: v for k, v in client_data.items() if k not in known_keys}
            settings.extra.update(extra)

            clients[client_id] = settings
    return observability, backend, clients


def load_config_dir(config_dir: str | Path, environment: str) -> RuntimeConfig:
    """
    Loads unified configuration from a directory.
    - {config_dir}/shared/ - shared base config
    - {config_dir}/{environment}/ - environment-specific overrides
    """

    # Preserve specific protected environment variables
    # to ensure system environment values take precedence over .env files.
    protected_keys = ["FLOWSTASH_API_URL", "FLOWSTASH_API_KEY"]
    protected_values = {
        key: os.environ[key] for key in protected_keys if key in os.environ
    }

    # Load .env file if it exists
    load_dotenv()

    print("Loading config for environment: ", environment)
    config_path = Path(config_dir)
    if not config_path.is_dir():
        raise ValueError(
            f"Config directory {config_dir} does not exist or is not a directory"
        )

    # Short-circuit for smoke-test environment: return a minimal, safe RuntimeConfig
    # This allows importing application modules during CI/build checks without requiring
    # real environment-specific configuration or secrets.
    try:
        env_up = str(environment).upper()
    except Exception:
        env_up = ""
    if env_up == "SMOKE-TEST":
        minimal_config = RuntimeConfig(
            backend=BackendConfig(),
            state_store=StateStoreConfig(),
        )
        init_registry(minimal_config)
        return minimal_config

    shared_path = config_path / "shared"
    env_path = config_path / environment

    # Load .env files from config dirs: shared first, then env-specific (env wins)
    shared_env_file = shared_path / ".env"
    if shared_env_file.exists():
        load_dotenv(shared_env_file, override=True)

    env_env_file = env_path / ".env"
    if env_env_file.exists():
        load_dotenv(env_env_file, override=True)

    # Re-introduce protected environment variables if they were set
    for key, value in protected_values.items():
        os.environ[key] = value

    # Load shared config
    obs_shared, backend_shared, clients_shared = None, None, {}
    if shared_path.is_dir():
        obs_shared, backend_shared, clients_shared = _load_data_from_dir(shared_path)

    # Load env config
    obs_env, backend_env, clients_env = None, None, {}
    if env_path.is_dir():
        obs_env, backend_env, clients_env = _load_data_from_dir(env_path)
    elif environment != "shared":
        raise ValueError(f"Environment directory {env_path} does not exist")

    # Merge — shared is the base; env-specific overrides individual fields (deep merge)
    observability_data = _deep_merge(obs_shared or {}, obs_env or {}) or None
    observability = (
        ObservabilityConfig.model_validate(observability_data)
        if observability_data
        else None
    )

    backend_yaml_env = _deep_merge(backend_shared or {}, backend_env or {})

    backend_data = backend_yaml_env.get("backend")
    backend_config = (
        BackendConfig.model_validate(backend_data) if backend_data else BackendConfig()
    )

    webhooks_data = backend_yaml_env.get("webhooks")
    webhooks_config = (
        WebhooksConfig.model_validate(webhooks_data)
        if webhooks_data
        else WebhooksConfig()
    )

    state_store_data = backend_yaml_env.get("state_store")
    state_store_config = (
        StateStoreConfig.model_validate(state_store_data)
        if state_store_data
        else StateStoreConfig()
    )

    # Clients: Merge dicts, Env wins on collision
    clients = {**clients_shared, **clients_env}

    config = RuntimeConfig(
        observability=observability,
        backend=backend_config,
        webhooks=webhooks_config,
        state_store=state_store_config,
        clients=clients,
    )

    init_registry(config)

    return config

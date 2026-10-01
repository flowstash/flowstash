from __future__ import annotations
from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, ConfigDict
from .observability_config import ObservabilityConfig
from flowstash.clients.config import ClientSettings
import os


class ClientsConfigPointer(BaseModel):
    path: str
    pattern: str = "*.yaml"
    recursive: bool = False

class BackendType(str, Enum):
    """Task queue backend types."""
    ASYNC = "asyncio"      # In-process async execution
    DRAMATIQ = "dramatiq"        # Dramatiq queue
    MANAGED = "managed"          # Managed platform (Cloud Tasks via API)

class DramatiqConfig(BaseModel):
    """Dramatiq backend configuration."""
    redis_url: Optional[str] = None # Redis connection string, overrides REDIS_URL env var if set

    model_config = ConfigDict(populate_by_name=True)

class AsyncConfig(BaseModel):
    """Asyncio backend configuration."""
    enable_scheduled_jobs: bool = True  # Set False to skip starting APScheduler for cron-scheduled tasks

    model_config = ConfigDict(populate_by_name=True)

class BackendConfig(BaseModel):
    """Configuration for task backend."""
    type: BackendType = BackendType.DRAMATIQ
    dramatiq: Optional[DramatiqConfig] = None
    async_: Optional[AsyncConfig] = Field(default=None, alias="async")

    model_config = ConfigDict(populate_by_name=True)

class WebhooksConfig(BaseModel):
    """Configuration for webhook ingress."""
    prefix: str = "/webhooks"

    model_config = ConfigDict(populate_by_name=True)


class StateStoreType(str, Enum):
    """State store backend types."""
    REDIS = "redis"
    SQLITE = "sqlite"
    MANAGED = "managed"


class SQLiteStoreConfig(BaseModel):
    """SQLite state store configuration."""
    db_path: str = ".flowstash_state.db"

    model_config = ConfigDict(populate_by_name=True)


class RedisStoreConfig(BaseModel):
    """Redis state store configuration."""
    # If None, falls back to REDIS_URL env var (same as DramatiqConfig)
    redis_url: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class ManagedStoreConfig(BaseModel):
    """Managed (HTTP) state store configuration."""
    base_url: Optional[str] = None  # Falls back to FLOWSTASH_API_URL env var
    api_key: Optional[str] = None   # Falls back to flowstash_API_KEY env var

    model_config = ConfigDict(populate_by_name=True)


class StateStoreConfig(BaseModel):
    """Top-level state store configuration block (backend.yaml: state_store:)."""
    type: StateStoreType = StateStoreType.REDIS
    sqlite: Optional[SQLiteStoreConfig] = None
    redis: Optional[RedisStoreConfig] = None
    managed: Optional[ManagedStoreConfig] = None

    model_config = ConfigDict(populate_by_name=True)

    def build_store(self):
        """Instantiate and return the configured StateStore implementation."""
        if self.type == StateStoreType.SQLITE:
            from flowstash.state.stores.sqlite_store import SQLiteStateStore
            cfg = self.sqlite or SQLiteStoreConfig()
            return SQLiteStateStore(db_path=cfg.db_path)

        if self.type == StateStoreType.REDIS:
            from flowstash.state.stores.redis_store import RedisStateStore
            cfg = self.redis or RedisStoreConfig()
            redis_url = cfg.redis_url or os.environ.get("REDIS_URL")
            return RedisStateStore(redis_url=redis_url)

        if self.type == StateStoreType.MANAGED:
            from flowstash.state.stores.managed_store import ManagedStateStore
            cfg = self.managed or ManagedStoreConfig()
            return ManagedStateStore(
                base_url=cfg.base_url,
                api_key=cfg.api_key,
            )

        raise ValueError(f"Unknown state store type: {self.type}")


class RuntimeConfig(BaseModel):
    observability: Optional[ObservabilityConfig] = None
    backend: BackendConfig = Field(default_factory=BackendConfig)
    clients: Dict[str, ClientSettings] = Field(default_factory=dict)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    state_store: StateStoreConfig = Field(default_factory=StateStoreConfig)

    model_config = ConfigDict(populate_by_name=True)

class RuntimeConfigRegistry:
    """
    A registry for the runtime configuration.
    """
    def __init__(self, config: RuntimeConfig):
        self._config = config
        self._clients = config.clients

    @property
    def observability(self) -> Optional[ObservabilityConfig]:
        return self._config.observability

    @property
    def backend(self) -> BackendConfig:
        return self._config.backend

    def get_client(self, client_id: str) -> Optional[ClientSettings]:
        return self._clients.get(client_id)

    def __getitem__(self, key: str) -> ClientSettings:
        if key not in self._clients:
            raise KeyError(f"Client '{key}' not found.")
        return self._clients[key]

    def __contains__(self, key: str) -> bool:
        return key in self._clients

    @property
    def clients(self) -> Dict[str, ClientSettings]:
        return self._clients

_global_registry: Optional[RuntimeConfigRegistry] = None

def get_global_registry() -> Optional[RuntimeConfigRegistry]:
    """Get the global runtime configuration registry."""
    return _global_registry

def set_global_registry(registry: RuntimeConfigRegistry):
    """Set the global runtime configuration registry."""
    global _global_registry
    _global_registry = registry

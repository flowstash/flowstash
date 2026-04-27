from __future__ import annotations
from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, ConfigDict
from .observability_config import ObservabilityConfig
from flowstash.clients.config import ClientSettings


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

class BackendConfig(BaseModel):
    """Configuration for task backend."""
    type: BackendType = BackendType.DRAMATIQ
    dramatiq: Optional[DramatiqConfig] = None
    
    model_config = ConfigDict(populate_by_name=True)

class WebhooksConfig(BaseModel):
    """Configuration for webhook ingress."""
    prefix: str = "/webhooks"
    

    model_config = ConfigDict(populate_by_name=True)

class RuntimeConfig(BaseModel):
    observability: Optional[ObservabilityConfig] = None
    backend: BackendConfig = Field(default_factory=BackendConfig)
    clients: Dict[str, ClientSettings] = Field(default_factory=dict)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)


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

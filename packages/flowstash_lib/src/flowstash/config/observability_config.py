from enum import Enum
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class DurabilityMode(str, Enum):
    IMMEDIATE = "immediate"
    EVENTUAL = "eventual"


class StoreType(str, Enum):
    DISABLED = "disabled"
    CONSOLE = "console"
    MANAGED = "managed"
    LOCAL = "local"


class LoggingConfig(BaseModel):
    enabled: bool = True
    min_level: str = "INFO"
    include_prefixes: list[str] = Field(default_factory=list, alias="includePrefixes")
    exclude_prefixes: list[str] = Field(default_factory=list, alias="excludePrefixes")
    filter_fn: Optional[str] = Field(None, alias="filterFn")

    model_config = ConfigDict(populate_by_name=True)


class ObservabilityConfig(BaseModel):
    durability: DurabilityMode = DurabilityMode.EVENTUAL
    store_type: StoreType = Field(StoreType.DISABLED, alias="storeType")

    # Local settings
    local_store_path: str = Field("logs", alias="localStorePath")

    # Managed API settings
    managed_api_url: Optional[str] = Field(None, alias="managedApiUrl")
    managed_api_key: Optional[str] = Field(None, alias="managedApiKey")
    project_id: Optional[str] = Field(None, alias="projectId")

    # Logging settings
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Worker settings
    # When True, the worker middleware calls AsyncManager.flush() + store.flush() after each
    # task completes so that all lifecycle events are durably written before the next message
    # is picked up.  Set to False only for very high-throughput, best-effort scenarios.
    flush_on_task_exit: bool = Field(True, alias="flushOnTaskExit")

    model_config = ConfigDict(populate_by_name=True)

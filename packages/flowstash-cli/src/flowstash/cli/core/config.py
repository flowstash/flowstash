from typing import List
from pydantic import Field
import os
from pathlib import Path
from typing import Optional, Dict, Any
import yaml
import keyring
from pydantic import BaseModel

flowstash_DIR = Path.home() / ".flowstash"
GLOBAL_CONFIG_FILE = flowstash_DIR / "config.yaml"
PROJECT_CONFIG_FILE = ".flowstash"

SERVICE_NAME = "flowstash"
TOKEN_KEY = "access_token"

class GlobalConfig(BaseModel):
    api_url: str = os.getenv("FLOWSTASH_API_URL", "https://api.flowstash.dev")  # Default to managed platform

class EnvironmentMode(BaseModel):
    name: str
    managed: bool = False
    options: Dict[str, str] = Field(default_factory=dict)

class ProjectConfig(BaseModel):
    project_name: Optional[str] = None
    project_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_email: Optional[str] = None
    environments: List[EnvironmentMode] = Field(default_factory=list)

def load_global_config() -> GlobalConfig:
    if not GLOBAL_CONFIG_FILE.exists():
        return GlobalConfig()
    
    try:
        with open(GLOBAL_CONFIG_FILE, "r") as f:
            data = yaml.safe_load(f) or {}
            return GlobalConfig(**data)
    except Exception:
        return GlobalConfig()

def save_global_config(config: GlobalConfig):
    flowstash_DIR.mkdir(parents=True, exist_ok=True)
    with open(GLOBAL_CONFIG_FILE, "w") as f:
        yaml.safe_dump(config.model_dump(), f)

def get_access_token() -> Optional[str]:
    return keyring.get_password(SERVICE_NAME, TOKEN_KEY)

def set_access_token(token: str):
    keyring.set_password(SERVICE_NAME, TOKEN_KEY, token)

def delete_access_token():
    try:
        keyring.delete_password(SERVICE_NAME, TOKEN_KEY)
    except keyring.errors.PasswordDeleteError:
        pass

def load_project_config() -> Optional[ProjectConfig]:
    path = Path(PROJECT_CONFIG_FILE)
    if not path.exists():
        return None
    
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
            return ProjectConfig(**data)
    except Exception:
        return None

def save_project_config(config: ProjectConfig):
    with open(PROJECT_CONFIG_FILE, "w") as f:
        yaml.safe_dump(config.model_dump(), f)

# Legacy aliases for compatibility if needed, though we should update callers
def load_config() -> GlobalConfig:
    return load_global_config()

def save_config(config: GlobalConfig):
    save_global_config(config)

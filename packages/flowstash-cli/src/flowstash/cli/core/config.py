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
TOKEN_KEY = "access_token"  # legacy key – kept for migration only


class UserAccount(BaseModel):
    email: str
    display_name: Optional[str] = None
    tenant_id: Optional[str] = None


class GlobalConfig(BaseModel):
    api_url: str = os.getenv("FLOWSTASH_API_URL", "https://api.flowstash.dev")
    accounts: List[UserAccount] = Field(default_factory=list)
    current_user: Optional[str] = None  # email of last-used account


class EnvironmentMode(BaseModel):
    name: str
    managed: bool = False
    options: Dict[str, str] = Field(default_factory=dict)


class ProjectConfig(BaseModel):
    project_name: Optional[str] = None
    project_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_email: Optional[str] = None
    linked_user: Optional[str] = None  # email pinned to this project
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


# ---------------------------------------------------------------------------
# Per-user token helpers
# ---------------------------------------------------------------------------

def _user_token_key(email: str) -> str:
    return f"token:{email}"


def set_token_for_user(email: str, token: str) -> None:
    keyring.set_password(SERVICE_NAME, _user_token_key(email), token)


def get_token_for_user(email: str) -> Optional[str]:
    return keyring.get_password(SERVICE_NAME, _user_token_key(email))


def delete_token_for_user(email: str) -> None:
    try:
        keyring.delete_password(SERVICE_NAME, _user_token_key(email))
    except keyring.errors.PasswordDeleteError:
        pass


def register_user(
    email: str,
    token: str,
    display_name: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> None:
    """Persist a new (or updated) login session without evicting other users."""
    set_token_for_user(email, token)
    config = load_global_config()
    existing = next((a for a in config.accounts if a.email == email), None)
    if existing:
        if display_name:
            existing.display_name = display_name
        if tenant_id:
            existing.tenant_id = tenant_id
    else:
        config.accounts.append(
            UserAccount(email=email, display_name=display_name, tenant_id=tenant_id)
        )
    config.current_user = email
    save_global_config(config)


def resolve_credentials(user: Optional[str] = None) -> Optional[str]:
    """
    Resolve an access token using the priority chain:
      1. Explicit user (--user flag)
      2. Project-linked user (.flowstash linked_user)
      3. Global current_user (last logged-in)
      4. Legacy 'access_token' keyring key (backward compat)
      5. FLOWSTASH_USER env var acts as implicit --user when set
    """
    # env-var override (useful for CI)
    effective_user = user or os.getenv("FLOWSTASH_USER")
    if effective_user:
        return get_token_for_user(effective_user)

    project_config = load_project_config()
    if project_config and project_config.linked_user:
        token = get_token_for_user(project_config.linked_user)
        if token:
            return token

    global_config = load_global_config()
    if global_config.current_user:
        token = get_token_for_user(global_config.current_user)
        if token:
            return token

    # Legacy migration path
    return keyring.get_password(SERVICE_NAME, TOKEN_KEY)


# ---------------------------------------------------------------------------
# Legacy helpers (kept for backward compat / tests)
# ---------------------------------------------------------------------------

def get_access_token() -> Optional[str]:
    """Backward-compatible accessor. Prefer resolve_credentials() for new code."""
    return resolve_credentials()


def set_access_token(token: str) -> None:
    """Legacy setter – stores under the old single-token key only."""
    keyring.set_password(SERVICE_NAME, TOKEN_KEY, token)


def delete_access_token(email: Optional[str] = None) -> None:
    """Log out one user (or the current user when email is None)."""
    if not email:
        config = load_global_config()
        email = config.current_user

    if email:
        delete_token_for_user(email)
        config = load_global_config()
        config.accounts = [a for a in config.accounts if a.email != email]
        if config.current_user == email:
            config.current_user = config.accounts[-1].email if config.accounts else None
        save_global_config(config)
    else:
        # Legacy fallback
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


# Legacy aliases
def load_config() -> GlobalConfig:
    return load_global_config()


def save_config(config: GlobalConfig):
    save_global_config(config)

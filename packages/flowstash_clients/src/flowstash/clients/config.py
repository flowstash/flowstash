from __future__ import annotations
import os
from enum import Enum
from typing import Dict, Any, Optional, Union, List
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator


class AuthType(str, Enum):
    BASIC = "basic"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    NONE = "none"
    CUSTOM = "custom"


class Behaviour(str, Enum):
    RAISE = "raise"
    MOCK = "mock"
    ALLOW = "allow"


class MockResponseConfig(BaseModel):
    status: int
    headers: Dict[str, str] = Field(default_factory=dict)
    content: Optional[str] = None
    file_ref: Optional[str] = Field(default=None, alias="file-ref")

    model_config = ConfigDict(populate_by_name=True)


class PolicyConfig(BaseModel):
    behaviour: Behaviour
    mock_response: Optional[MockResponseConfig] = Field(
        default=None, alias="mock-response"
    )

    model_config = ConfigDict(populate_by_name=True)


class SuppressionRule(BaseModel):
    path: str
    methods: Dict[str, PolicyConfig] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def capture_methods(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        # If "methods" is already present and is a dict, we might be re-validating
        # or it might be coming from a code instantiation where 'methods' was passed.
        if "methods" in data and isinstance(data["methods"], dict) and len(data) == 2:
            return data

        # Extract fixed fields
        path = data.get("path")

        # Everything else that matches HTTP methods (or *) goes into methods
        methods = data.get("methods", {})
        if not isinstance(methods, dict):
            methods = {}

        for key, value in data.items():
            if key in ("path", "methods"):
                continue

            # Treat all other keys as potential method specifiers
            # This handles GET, POST, *, etc.
            methods[key] = value

        return {"path": path, "methods": methods}


class BasicAuthConfig(BaseModel):
    type: AuthType = AuthType.BASIC
    username: str
    password: str


class ApiKeyLocation(str, Enum):
    HEADER = "header"
    QUERY = "query"


class ApiKeyAuthConfig(BaseModel):
    type: AuthType = AuthType.API_KEY
    key: str
    value: str
    in_: ApiKeyLocation = Field(default=ApiKeyLocation.HEADER, alias="in")

    model_config = ConfigDict(populate_by_name=True)


class OAuth2ClientAuthMethod(str, Enum):
    CLIENT_SECRET_BASIC = (
        "client_secret_basic"  # Credentials as Basic Auth header (default)
    )
    CLIENT_SECRET_POST = "client_secret_post"  # Credentials in request body


class OAuth2AuthConfig(BaseModel):
    type: AuthType = AuthType.OAUTH2
    grant_type: str = Field(default="client_credentials", alias="grantType")
    client_id: str
    client_secret: str
    token_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    refresh_token: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)
    extra_params: Dict[str, Any] = Field(default_factory=dict)
    client_auth_method: OAuth2ClientAuthMethod = Field(
        default=OAuth2ClientAuthMethod.CLIENT_SECRET_BASIC,
        alias="clientAuthMethod",
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def infer_grant_type(cls, data: Any) -> Any:
        if isinstance(data, dict):
            grant_type = data.get("grant_type") or data.get("grantType")
            if not grant_type or grant_type == "client_credentials":
                if "username" in data or "password" in data:
                    data["grant_type"] = "password"
        return data


class NoneAuthConfig(BaseModel):
    type: AuthType = AuthType.NONE


class CustomAuthConfig(BaseModel):
    type: AuthType = AuthType.CUSTOM
    extra: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def capture_extra(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        # If 'extra' is already present, we might be re-validating
        if "extra" in data and len(data) == 2:
            return data

        type_val = data.get("type")
        extra = {k: v for k, v in data.items() if k != "type"}
        return {"type": type_val, "extra": extra}


AuthConfig = Union[
    BasicAuthConfig,
    ApiKeyAuthConfig,
    OAuth2AuthConfig,
    NoneAuthConfig,
    CustomAuthConfig,
]


class RetryConfig(BaseModel):
    max_retries: int = Field(default=0, alias="maxRetries")
    max_wait: float = Field(default=60.0, alias="maxWait")
    whitelist: List[str] = Field(default_factory=list)
    """A list of keywords to match in response for retrying. If empty, all errors are retried."""
    blacklist: List[str] = Field(default_factory=list)
    """A list of keywords to match in response for not retrying. Takes precedence over whitelist. If empty, no errors are excluded from retrying. """

    model_config = ConfigDict(populate_by_name=True)


class TLSConfig(BaseModel):
    """TLS / certificate configuration for an HTTP client."""

    # Client certificate for mutual TLS (mTLS)
    cert_file: Optional[str] = Field(default=None, alias="certFile")
    key_file: Optional[str] = Field(default=None, alias="keyFile")

    # Server certificate verification
    ca_bundle: Optional[str] = Field(default=None, alias="caBundle")
    verify_ssl: bool = Field(default=True, alias="verifySSL")

    model_config = ConfigDict(populate_by_name=True)


class ClientSettings(BaseModel):
    client_id: str = Field(alias="client_id")
    base_url: str = Field(alias="baseUrl")
    auth: Optional[AuthConfig] = None
    timeout: float = 10.0
    backend: Optional[str] = None
    dsn: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)

    # Client-wide configuration
    retry: RetryConfig = Field(default_factory=RetryConfig)
    handle_redirects: bool = Field(default=False, alias="handleRedirects")
    tls: Optional[TLSConfig] = Field(default=None)
    suppress: List[SuppressionRule] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class ClientConfigRegistry:
    """
    A read-only registry for client configurations.
    """

    def __init__(self, config_dict: Dict[str, Any]):
        self._clients: Dict[str, ClientSettings] = {}
        clients_data = config_dict.get("clients", {})

        for name, settings in clients_data.items():
            # In the new flat model, we just validate the settings directly
            if "client_id" not in settings:
                settings["client_id"] = name
            self._clients[name] = ClientSettings.model_validate(settings)

    def __getitem__(self, key: str) -> ClientSettings:
        if key not in self._clients:
            raise KeyError(f"Client '{key}' not found in configuration.")
        return self._clients[key]

    def get(self, key: str, default: Any = None) -> Optional[ClientSettings]:
        return self._clients.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._clients

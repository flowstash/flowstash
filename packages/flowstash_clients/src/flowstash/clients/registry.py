from typing import TYPE_CHECKING, Dict, Type, Optional, Any, List

if TYPE_CHECKING:
    from .base import BaseClient

# We'll use a Protocol or just Any for RuntimeConfig to avoid circular dependencies if possible,
# or just assume it has a .clients property that is a dict of ClientRecord-like objects.


class ClientRegistry:
    _instance = None

    def __init__(self, config: Any):
        self._config = config
        self._clients: Dict[str, "BaseClient"] = {}
        self._class_map: Dict[str, Type["BaseClient"]] = {}

    @classmethod
    def initialize(cls, config: Any):
        if cls._instance is None:
            cls._instance = cls(config)
        # Process deferred
        for name, client_cls in _deferred_registrations:
            cls._instance.register_client_class(name, client_cls)
        _deferred_registrations.clear()

    @classmethod
    def get_instance(cls) -> "ClientRegistry":
        if not cls._instance:
            raise RuntimeError(
                "ClientRegistry not initialized. Call init_registry() first."
            )
        return cls._instance

    @classmethod
    def get_instance_safe(cls) -> Optional["ClientRegistry"]:
        return cls._instance

    def register_client_class(self, name: str, cls: Type["BaseClient"]):
        self._class_map[name] = cls

    def get_client(self, client_id: str) -> "BaseClient":
        if client_id in self._clients:
            return self._clients[client_id]

        # Create client
        if not hasattr(self._config, "clients") or not self._config.clients:
            raise KeyError(f"No clients configured. Cannot find '{client_id}'.")

        # In RuntimeConfig, clients is Dict[str, ClientSettings]
        client_record = self._config.clients.get(client_id)
        if not client_record:
            raise KeyError(
                f"Client '{client_id}' not found in configuration. Available clients: {list(self._config.clients.keys())}"
            )

        # Determine class:
        # 1. Look in registered class map (by client_id)
        # 2. Default to "BaseClient"
        client_class = self._class_map.get(client_id, "BaseClient")

        # Instantiate
        client = client_class(name=client_id, settings=client_record)
        self._clients[client_id] = client
        return client


def init_registry(config: Any):
    ClientRegistry.initialize(config)


def register_client_class(name: str, cls: Type["BaseClient"]):
    # If initialized, register immediately
    registry = ClientRegistry.get_instance_safe()
    if registry:
        registry.register_client_class(name, cls)
    else:
        _deferred_registrations.append((name, cls))


_deferred_registrations: List[tuple[str, Type["BaseClient"]]] = []


def client(name: str):
    """
    Decorator to register a client class.

    @client("demoClient")
    class DemoClient("BaseClient"):
        ...
    """

    def decorator(cls):
        register_client_class(name, cls)
        cls._client_name = name
        return cls

    return decorator


def get_client(client_id: str) -> Any:
    return ClientRegistry.get_instance().get_client(client_id)

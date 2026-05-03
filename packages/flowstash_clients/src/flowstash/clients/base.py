from typing import TypeVar, Type, cast

from .registry import get_client

T = TypeVar("T", bound="BaseClient")


class BaseClient:

    @classmethod
    def get_client(cls: Type[T]) -> T:
        if getattr(cls, "_client_name", None) is None:
            raise RuntimeError(
                "Client class not registered with a name. use @client decorator to register."
            )
        return cast(T, get_client(cls._client_name))

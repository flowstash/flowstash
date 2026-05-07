from typing import TypeVar, Type, cast, Optional, TYPE_CHECKING

from .registry import get_client

if TYPE_CHECKING:
    from flowstash.observability.model import DataExchangeEvent, Correlation

T = TypeVar("T", bound="BaseClient")


class BaseClient:

    @classmethod
    def get_client(cls: Type[T]) -> T:
        if getattr(cls, "_client_name", None) is None:
            raise RuntimeError(
                "Client class not registered with a name. use @client decorator to register."
            )
        return cast(T, get_client(cls._client_name))

    def mask_sensitive_data(self, event: "DataExchangeEvent") -> "DataExchangeEvent":
        """
        Override in subclasses to redact secrets from the event before emission.
        Default implementation is a pass-through (no masking).
        """
        return event

    async def _emit_data_exchange(
        self,
        event: "DataExchangeEvent",
        correlation: "Optional[Correlation]" = None,
    ) -> None:
        """
        Mask then emit a DataExchangeEvent.
        All subclass call sites should use this instead of calling
        record_data_exchange directly.
        """
        from flowstash.observability.ingestion import record_data_exchange
        masked = self.mask_sensitive_data(event)
        await record_data_exchange(masked, correlation)

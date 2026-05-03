import os
from typing import Tuple, Optional
from .model import RunEvent, SpanEvent, Correlation, DataExchange, RecordLink
from .stores.protocols import EventsStore, DataExchangeStore, BlobStore, RecordsStore

from ..config.observability_config import ObservabilityConfig, StoreType


class NoOpEventsStore(EventsStore):
    def write_run_event(self, event: RunEvent) -> None:
        pass

    def write_span_event(self, event: SpanEvent) -> None:
        pass

    def write_log(
        self,
        correlation: Correlation,
        severity: str,
        message: str,
        attrs: dict | None = None,
    ) -> None:
        pass


class NoOpRecordsStore(RecordsStore):
    def write_record_link(self, link: RecordLink) -> None:
        pass


class NoOpDataExchangeStore(DataExchangeStore):

    def write_data_exchange(self, dx: DataExchange) -> None:
        pass


class NoOpBlobStore(BlobStore):
    def put(
        self, *, path_hint: str, content_type: str, data: bytes
    ) -> Tuple[str, int, str]:
        # Return dummy values
        return f"noop://{path_hint}", len(data), "noop-sha"


class ConsoleEventsStore(EventsStore):
    def write_run_event(self, event: RunEvent) -> None:
        print(f"[OBSERVABILITY] Run Event: {event.event_type} - {event.status}")

    def write_span_event(self, event: SpanEvent) -> None:
        print(
            f"[OBSERVABILITY] Span Event: {event.name} - {event.event_type} - {event.status}"
        )

    def write_log(
        self,
        correlation: Correlation,
        severity: str,
        message: str,
        attrs: dict | None = None,
    ) -> None:
        print(f"[OBSERVABILITY] Log: {severity} - {message}")


class ConsoleBlobStore(BlobStore):
    def put(
        self, *, path_hint: str, content_type: str, data: bytes
    ) -> Tuple[str, int, str]:
        print(f"[OBSERVABILITY] Blob Put: {path_hint} ({len(data)} bytes)")
        return f"console://{path_hint}", len(data), "sha256-dummy"

    def get(self, payload_ref: str) -> bytes:
        print(f"[OBSERVABILITY] Blob Get: {payload_ref}")
        return b""


class ConsoleDataExchangeStore(DataExchangeStore):
    def write_data_exchange(self, dx: DataExchange) -> None:
        print(f"[OBSERVABILITY] Data Exchange: {dx.id} - {dx.direction}")


class ConsoleRecordsStore(RecordsStore):
    def write_record_link(self, link: RecordLink) -> None:
        print(f"[OBSERVABILITY] Record Link: {link.record_id} -> {link.integration_id}")


_events_store: EventsStore = NoOpEventsStore()
_dx_store: DataExchangeStore = NoOpDataExchangeStore()
_blob_store: BlobStore = NoOpBlobStore()
_records_store: RecordsStore = NoOpRecordsStore()


def configure(config: ObservabilityConfig):
    global _events_store, _dx_store, _blob_store, _records_store
    print(f"Observability config: {config.store_type}")
    if config.store_type == StoreType.DISABLED:
        _events_store = NoOpEventsStore()
        _dx_store = NoOpDataExchangeStore()
        _blob_store = NoOpBlobStore()
        _records_store = NoOpRecordsStore()

    elif config.store_type == StoreType.CONSOLE:
        _events_store = ConsoleEventsStore()
        _dx_store = ConsoleDataExchangeStore()
        _blob_store = ConsoleBlobStore()
        _records_store = ConsoleRecordsStore()

    elif config.store_type == StoreType.LOCAL:
        from .stores.file_stores import FileStore

        store = FileStore(config.local_store_path)
        _events_store = store
        _dx_store = store
        _blob_store = store
        _records_store = store

    elif config.store_type == StoreType.MANAGED:
        from .stores.api_stores import (
            ApiEventsStore,
            ApiDataExchangeStore,
            ApiRecordsStore,
            ApiBlobStore,
        )

        if not config.managed_api_url:
            config.managed_api_url = os.getenv(
                "FLOWSTASH_API_URL", "https://api.flowstash.com"
            )

        _events_store = ApiEventsStore(config.managed_api_url, config.managed_api_key)
        _dx_store = ApiDataExchangeStore(config.managed_api_url, config.managed_api_key)
        _records_store = ApiRecordsStore(config.managed_api_url, config.managed_api_key)
        _blob_store = ApiBlobStore(config.managed_api_url, config.managed_api_key)

    elif config.store_type == "gcp":
        raise ValueError(
            "GCP store type is no longer supported in the client library. Use MANAGED instead."
        )


def get_events_store() -> EventsStore:
    return _events_store


def set_events_store(store: EventsStore) -> None:
    global _events_store
    _events_store = store


def get_data_exchange_store() -> DataExchangeStore:
    return _dx_store


def set_data_exchange_store(store: DataExchangeStore) -> None:
    global _dx_store
    _dx_store = store


def get_blob_store() -> BlobStore:
    return _blob_store


def set_blob_store(store: BlobStore) -> None:
    global _blob_store
    _blob_store = store


def get_records_store() -> RecordsStore:
    return _records_store


def set_records_store(store: RecordsStore) -> None:
    global _records_store
    _records_store = store


def flush_stores(timeout: float = 10.0) -> None:
    """Flush all stores that support it (e.g. drain _AsyncWorker HTTP queues)."""
    for store in (_events_store, _dx_store, _records_store, _blob_store):
        flush = getattr(store, "flush", None)
        if callable(flush):
            try:
                flush(timeout=timeout)
            except Exception:
                pass

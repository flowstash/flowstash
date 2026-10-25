from typing import List, Protocol, Tuple
from ..model import RunEvent, SpanEvent, Correlation, DataExchange, RecordLink

class RecordsStore(Protocol):
    # True when write_record_link only appends to an in-memory buffer, so it is
    # safe to call on the caller's thread (no IO on the hot path).
    buffered: bool = False

    def write_record_link(self, link: RecordLink) -> None:
        ...

    def write_record_links(self, links: List[RecordLink]) -> None:
        for link in links:
            self.write_record_link(link)


class EventsStore(Protocol):

    def write_run_event(self, event: RunEvent) -> None:
        ...

    def write_span_event(self, event: SpanEvent) -> None:
        ...

    def write_log(self, correlation: Correlation, severity: str, message: str, attrs: dict | None = None) -> None:
        ...


class DataExchangeStore(Protocol):
    def write_data_exchange(self, dx: DataExchange) -> None:
        ...


class BlobStore(Protocol):
    def put(self, *, path_hint: str, content_type: str, data: bytes) -> Tuple[str, int, str]:
        """
        Stores data and returns metadata.

        Args:
            path_hint: Suggested path structure (e.g. run_id/incoming/dx_id)
            content_type: MIME type of the content
            data: The bytes to store

        Returns:
            Tuple containing:
            - payload_ref: The reference string (e.g. gs://bucket/key)
            - size_bytes: Size of the stored data
            - sha256: SHA256 hash of the data
        """
        ...
    def get(self, payload_ref: str) -> bytes:
        """
        Retrieves data from a reference.

        Args:
            payload_ref: The reference string (e.g. gs://bucket/key)

        Returns:
            The retrieved bytes
        """
        ...

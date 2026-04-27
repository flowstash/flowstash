import os
import json
import hashlib
from datetime import datetime, UTC
from pathlib import Path
from typing import Tuple, Dict, Any, Optional
from dataclasses import asdict

from ..model import RunEvent, SpanEvent, Correlation, DataExchange, RecordLink
from .protocols import EventsStore, DataExchangeStore, BlobStore, RecordsStore


class FileStore(EventsStore, DataExchangeStore, BlobStore, RecordsStore):

    def __init__(self, base_dir: str = "logs"):
        self.base_dir = Path(base_dir).absolute()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_run_dir(self, correlation: Correlation) -> Path:
        # User said: [isoDate]_[runId]/
        # isoDate usually means YYYY-MM-DD
        date_str = datetime.now(UTC).date().isoformat()
        run_id = correlation.run_id or "unknown"
        run_dir = self.base_dir / f"{date_str}_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _write_file(self, run_dir: Path, correlation: Correlation, event_type: str, data: Dict[str, Any], occurred_at: datetime):
        # User said: starting with isodata, then run_id then span_id then type
        iso_data = occurred_at.strftime("%Y%m%dT%H%M%S.%f")
        run_id = correlation.run_id or "unknown"
        span_id = correlation.span_id or "root"
        filename = f"{iso_data}_{run_id}_{span_id}_{event_type}.json"
        
        # Ensure directory exists even if it was deleted
        run_dir.mkdir(parents=True, exist_ok=True)
        
        with open(run_dir / filename, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, indent=2)

    def write_run_event(self, event: RunEvent) -> None:
        run_dir = self._get_run_dir(event.correlation)
        self._write_file(run_dir, event.correlation, f"RUN_{event.event_type}", asdict(event), event.occurred_at)

    def write_span_event(self, event: SpanEvent) -> None:
        run_dir = self._get_run_dir(event.correlation)
        self._write_file(run_dir, event.correlation, f"SPAN_{event.event_type}", asdict(event), event.occurred_at)

    def write_log(self, correlation: Correlation, severity: str, message: str, attrs: dict | None = None) -> None:
        run_dir = self._get_run_dir(correlation)
        now = datetime.now(UTC)
        data = {
            "severity": severity,
            "message": message,
            "attrs": attrs or {},
            "correlation": asdict(correlation),
            "occurred_at": now.isoformat()
        }
        self._write_file(run_dir, correlation, "LOG", data, now)

    def write_data_exchange(self, dx: DataExchange) -> None:
        run_dir = self._get_run_dir(dx.correlation)
        self._write_file(run_dir, dx.correlation, "DX", asdict(dx), dx.occurred_at)

    def write_record_link(self, link: RecordLink) -> None:
        # For RecordLink, we can use a dummy correlation for _get_run_dir if run_id is not set
        # but usually it should be set.
        dummy_corr = Correlation(run_id=link.run_id)
        run_dir = self._get_run_dir(dummy_corr)
        self._write_file(run_dir, dummy_corr, "RECORD_LINK", asdict(link), link.event_time)


    def put(self, *, path_hint: str, content_type: str, data: bytes) -> Tuple[str, int, str]:
        # path_hint is typically {run_id}/dx/{dx_id}/request
        # To be consistent with events, we prefix with isoDate
        date_str = datetime.now(UTC).date().isoformat()
        target_path = self.base_dir / f"{date_str}_{path_hint}"
        
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(target_path, "wb") as f:
            f.write(data)
            
        sha = hashlib.sha256(data).hexdigest()
        
        return f"file://{target_path}", len(data), sha

        
    def get(self, payload_ref: str) -> bytes:
        if payload_ref.startswith("file://"):
            path = Path(payload_ref[7:])
            with open(path, "rb") as f:
                return f.read()
        raise ValueError(f"Unsupported payload_ref: {payload_ref}")

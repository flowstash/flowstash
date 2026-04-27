from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

@dataclass
class RecordData:
    record_id: str
    record_type: str
    data: Any
    timestamp: Optional[datetime] = None
    dedupe_key: Optional[str] = None
    # Source context — populated automatically by RecordsFeed.publish()
    source_integration: Optional[str] = None
    source_pipeline: Optional[str] = None
    source_run_id: Optional[str] = None
    source_traceparent: Optional[str] = None

    def get_dedupe_key(self, integration: str) -> str:
        if self.dedupe_key:
            return self.dedupe_key
        # default_dedupe_key = (integration, record_type, record_id)
        return f"{integration}:{self.record_type}:{self.record_id}"

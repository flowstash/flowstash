from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class RecordData:
    record_id: str
    record_type: str
    data: Any
    timestamp: Optional[datetime] = None
    dedupe_key: Optional[str] = None
    # Source context — auto-resolved from integration_context if not provided
    source_integration: Optional[str] = field(default=None)
    source_pipeline: Optional[str] = field(default=None)
    source_run_id: Optional[str] = field(default=None)
    source_traceparent: Optional[str] = field(default=None)

    def __post_init__(self):
        if any(
            v is None
            for v in (
                self.source_integration,
                self.source_pipeline,
                self.source_run_id,
                self.source_traceparent,
            )
        ):
            from flowstash.context import current_context

            ctx = current_context()
            if ctx is not None:
                if self.source_integration is None:
                    self.source_integration = ctx.integration
                if self.source_pipeline is None:
                    self.source_pipeline = ctx.integration_pipeline
                if self.source_run_id is None:
                    self.source_run_id = ctx.run_id
                if self.source_traceparent is None:
                    self.source_traceparent = ctx.traceparent

    def get_dedupe_key(self, integration: str) -> str:
        if self.dedupe_key:
            return self.dedupe_key
        # default_dedupe_key = (integration, record_type, record_id)
        return f"{integration}:{self.record_type}:{self.record_id}"

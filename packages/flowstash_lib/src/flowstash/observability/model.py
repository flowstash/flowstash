from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Optional, Literal, Union, Dict
from enum import Enum
import uuid


@dataclass(frozen=True)
class Correlation:

    # optional integration field
    integration: Optional[str] = None
    integration_pipeline: Optional[str] = None

    run_id: str = ""  # must be set

    # Project / environment context (injected by the observability store for self-hosted)
    project_id: Optional[str] = None
    environment: Optional[str] = None

    # Trace context (optional)
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None

    # Causal chain (optional)
    # parent_run_id: the run that triggered this execution via submit/schedule
    parent_run_id: Optional[str] = None
    # operation_id: stable join key generated at delegation time, before execution run_id exists
    operation_id: Optional[str] = None

    # Runtime semantics (optional)
    step_key: Optional[str] = None
    attempt: Optional[int] = None

    # lightweight tags (safe keys only)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskDelegationMetadata:
    """Causal envelope carried in queue message headers from triggering run to executing run."""

    parent_run_id: str
    # ^ Run that created this delegation. Preserved on the child execution run.

    operation_id: str
    # ^ Stable UUID generated before the backend is called. Primary join key between
    #   the delegation span (on the parent run) and the child execution run.

    target_task: str
    # ^ Auto-derived module.function_name of the delegated task. Carried for span display.

    accepted_id: Optional[str] = None
    # ^ Opaque handle returned by the backend after acceptance (e.g. Dramatiq message_id,
    #   Cloud Tasks task_id). Set after the backend call. Secondary join key when present.

    schedule_time: Optional[datetime] = None
    # ^ Only set for .schedule() calls. The time the backend was asked to run the task.

    attrs: dict = field(default_factory=dict)
    # ^ Free-form backend-specific metadata (e.g. attrs["scheduled_job_id"] for cron identity).


@dataclass(frozen=True)
class Run:
    correlation: Correlation
    scheduled_job_id: Optional[str] = None

    artifact_id: Optional[str] = None
    env_snapshot_id: Optional[str] = None

    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: Optional[str] = None  # RUNNING/SUCCEEDED/FAILED/CANCELLED etc.

    # optional counters/metadata
    attrs: dict = None


@dataclass(frozen=True)
class Span:
    correlation: Correlation

    name: str
    start_time: datetime
    end_time: datetime
    status: str  # OK/ERROR/SKIPPED etc.

    error_summary: Optional[str] = None
    attrs: dict = None


Channel = Literal["HTTP", "FTP", "S3", "SFTP", "MQ", "FILE", "CUSTOM"]
State = Literal["STARTED", "SUCCEEDED", "FAILED", "RETRIED", "TIMEOUT", "CANCELLED"]


@dataclass(frozen=True)
class DataExchange:
    id: str  # uuid
    correlation: Correlation

    integration: str
    channel: Channel
    operation: str
    remote_system: Optional[str]
    address: str

    occurred_at: datetime
    completed_at: Optional[datetime]
    state: State
    attempt: int = 1
    retry_of_id: Optional[str] = None
    duration_ms: Optional[int] = None

    http_method: Optional[str] = None
    status_code: Optional[int] = None

    request_payload_ref: Optional[str] = None
    response_payload_ref: Optional[str] = None
    request_content_type: Optional[str] = None
    response_content_type: Optional[str] = None
    request_size_bytes: Optional[int] = None
    response_size_bytes: Optional[int] = None

    # Inline payload bytes (present when server handles storage; mutually exclusive with *_ref)
    request_payload: Optional[bytes] = None
    response_payload: Optional[bytes] = None

    request_headers: Optional[Dict[str, str]] = None
    response_headers: Optional[Dict[str, str]] = None

    attrs: dict = None


EventType = Literal["STARTED", "PROGRESS", "ENDED", "SCHEDULED"]


@dataclass(frozen=True)
class RunEvent:
    event_type: EventType
    correlation: Correlation
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    scheduled_job_id: Optional[str] = None

    # Optional fields depending on event type
    artifact_id: Optional[str] = None
    env_snapshot_id: Optional[str] = None
    status: str = "RUNNING"
    attrs: dict = field(default_factory=dict)
    finished_at: Optional[datetime] = (
        None  # Specific for ENDED if needed, or just use occurred_at
    )


@dataclass(frozen=True)
class SpanEvent:
    event_type: EventType
    correlation: Correlation
    name: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    status: str = "STARTED"

    # Optional fields
    start_time: Optional[datetime] = None  # For ENDED/duration
    end_time: Optional[datetime] = None
    error_summary: Optional[str] = None
    attrs: dict = field(default_factory=dict)


@dataclass(frozen=True)
class DataExchangeEvent:
    integration: str
    operation: str
    channel: Channel
    address: str
    remote_system: Optional[str] = None
    occurred_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    state: State = "SUCCEEDED"
    attempt: int = 1
    retry_of_id: Optional[str] = None
    http_method: Optional[str] = None
    status_code: Optional[int] = None
    request_payload: Optional[bytes] = None
    response_payload: Optional[bytes] = None
    request_content_type: Optional[str] = None
    response_content_type: Optional[str] = None
    request_size_bytes: Optional[int] = None
    response_size_bytes: Optional[int] = None
    offload_payloads: bool = (
        False  # if True, upload request/response payloads to blob store
    )
    request_headers: Optional[Dict[str, str]] = None
    response_headers: Optional[Dict[str, str]] = None
    attrs: dict = field(default_factory=dict)


class RecordLinkKind(str, Enum):
    PUBLISHED = "PUBLISHED"
    CONSUMED = "CONSUMED"
    LINKED = "LINKED"


@dataclass(frozen=True)
class RecordLink:
    tenant_id: str
    project_id: Optional[str] = None
    environment: Optional[str] = None
    integration: Optional[str] = None
    pipeline: Optional[str] = None
    run_id: str = ""
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    record_key: str = ""  # (integration, record_type, record_id)
    event_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    kind: RecordLinkKind = RecordLinkKind.PUBLISHED
    source: Optional[str] = None  # e.g. feed_id

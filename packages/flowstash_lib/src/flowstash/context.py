from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Mapping, Optional, TYPE_CHECKING
import contextvars
import uuid

if TYPE_CHECKING:
    from .integration.state import StateHandle


@dataclass(frozen=True)
class IntegrationContext:
    integration: str
    integration_pipeline: Optional[str]
    run_id: str
    traceparent: Optional[str] = None
    tracestate: Optional[str] = None
    baggage: Mapping[str, str] = field(default_factory=dict)
    tags: Mapping[str, Any] = field(default_factory=dict)
    tenant_id: str = "default"
    current_record_key: Optional[str] = None
    ingress_name: Optional[str] = None
    parent_span_id: Optional[str] = None
    # Causal chain fields — set when this context represents a delegated execution
    parent_run_id: Optional[str] = None
    operation_id: Optional[str] = None

    @property
    def corelation(self) -> Any:
        # Import inside to avoid circular dependencies
        from .observability.model import Correlation

        return Correlation(
            integration=self.integration,
            integration_pipeline=self.integration_pipeline,
            run_id=self.run_id,
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            parent_run_id=self.parent_run_id,
            operation_id=self.operation_id,
            tags={k: str(v) for k, v in self.tags.items()},
        )

    @property
    def trace_id(self) -> Optional[str]:
        if self.traceparent:
            parts = self.traceparent.split("-")
            if len(parts) >= 2:
                return parts[1]
        return None

    @property
    def span_id(self) -> Optional[str]:
        if self.traceparent:
            parts = self.traceparent.split("-")
            if len(parts) >= 3:
                return parts[2]
        return None


_current_ctx: contextvars.ContextVar[Optional[IntegrationContext]] = (
    contextvars.ContextVar("framework.integration_context", default=None)
)

_state_handle: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar(
    "flowstash.state_handle", default=None
)


def current_context() -> Optional[IntegrationContext]:
    """Return the current integration context."""
    return _current_ctx.get()


get_context = current_context


def set_context(ctx: IntegrationContext) -> Any:
    """Set the current integration context. Returns a token for resetting."""
    return _current_ctx.set(ctx)


def reset_context(token: Any):
    """Reset the integration context to its previous state."""
    _current_ctx.reset(token)


class integration_context:
    """
    Context manager for setting/creating an IntegrationContext.

    Lifecycle rule (automatic — no parameters needed):
    - If no active run exists (current_context() is None): records RunEvent(STARTED/ENDED).
    - If already inside a run: records SpanEvent(STARTED/ENDED) using span_name.
    """

    def __init__(
        self,
        integration: Optional[str] = None,
        integration_pipeline: Optional[str] = None,
        run_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        current_record_key: Optional[str] = None,
        ingress_name: Optional[str] = None,
        span_name: Optional[str] = None,
        record_lifecycle: bool = True,
        parent_run_id: Optional[str] = None,
        operation_id: Optional[str] = None,
        **kwargs,
    ):
        self.span_name = span_name
        self._record_lifecycle = record_lifecycle
        self.overrides = {
            "integration": integration,
            "integration_pipeline": integration_pipeline,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "ingress_name": ingress_name,
            "parent_run_id": parent_run_id,
            "operation_id": operation_id,
            "attrs": kwargs.pop("attrs", None),
            "metadata": kwargs.pop("metadata", None),
            **kwargs,
        }

        self.token = None
        self.state_token = None
        self._is_root_run: bool = False
        self._recorded_span_name: Optional[str] = None
        self._start_time: Optional[datetime] = None
        self._last_ctx: Optional[IntegrationContext] = None

    def __enter__(self) -> IntegrationContext:
        # Capture parent BEFORE setting the new context so auto-detection is correct.
        parent = current_context()
        self._is_root_run = parent is None
        self._start_time = datetime.now(UTC)

        if parent:
            # Reuse parent values if not overridden.
            # Advance span_id so each child context has its own span.
            import secrets

            new_span_id = secrets.token_hex(8)
            # Rebuild traceparent keeping the same trace_id, new span_id
            if parent.traceparent:
                parts = parent.traceparent.split("-")
                new_traceparent = f"{parts[0]}-{parts[1]}-{new_span_id}-{parts[3] if len(parts) > 3 else '01'}"
            else:
                trace_id = secrets.token_hex(16)
                new_traceparent = f"00-{trace_id}-{new_span_id}-01"
            merged = {
                "integration": self.overrides.get("integration") or parent.integration,
                "integration_pipeline": self.overrides.get("integration_pipeline")
                or parent.integration_pipeline,
                "run_id": self.overrides.get("run_id") or parent.run_id,
                "tenant_id": self.overrides.get("tenant_id") or parent.tenant_id,
                "traceparent": new_traceparent,
                "tracestate": parent.tracestate,
                "baggage": {**parent.baggage, **(self.overrides.get("baggage") or {})},
                "tags": {**parent.tags, **(self.overrides.get("tags") or {})},
                "current_record_key": self.overrides.get("current_record_key")
                or parent.current_record_key,
                "ingress_name": self.overrides.get("ingress_name")
                or parent.ingress_name,
                "parent_span_id": parent.span_id,
                # Inherit causal metadata from parent run (same run, different span)
                "parent_run_id": parent.parent_run_id,
                "operation_id": parent.operation_id,
            }

        else:
            # New root context — generate a W3C traceparent so trace_id and span_id
            # are always available even without an external OTel propagator.
            import secrets

            trace_id = secrets.token_hex(16)  # 128-bit / 32 hex chars
            span_id = secrets.token_hex(8)  # 64-bit  / 16 hex chars
            merged = {
                "integration": self.overrides.get("integration") or "unknown",
                "integration_pipeline": self.overrides.get("integration_pipeline"),
                "run_id": self.overrides.get("run_id") or str(uuid.uuid4()),
                "tenant_id": self.overrides.get("tenant_id") or "default",
                "traceparent": f"00-{trace_id}-{span_id}-01",
                "baggage": self.overrides.get("baggage") or {},
                "tags": self.overrides.get("tags") or {},
                "current_record_key": self.overrides.get("current_record_key"),
                "ingress_name": self.overrides.get("ingress_name"),
                # Causal metadata for delegated executions
                "parent_run_id": self.overrides.get("parent_run_id"),
                "operation_id": self.overrides.get("operation_id"),
            }

        ctx = IntegrationContext(**merged)
        self._last_ctx = ctx
        self.token = set_context(ctx)

        # Auto-record lifecycle events
        from .observability.ingestion import (
            _enqueue_lifecycle,
            record_run_started,
            record_span_started,
        )

        if self._record_lifecycle:
            corr = ctx.corelation
            if self._is_root_run:
                entry_point = self.span_name
                _enqueue_lifecycle(
                    record_run_started,
                    correlation=corr,
                    entry_point=entry_point,
                    attrs=self.overrides.get("attrs"),
                    metadata=self.overrides.get("metadata"),
                )
            else:
                name = (
                    self.span_name
                    or self.overrides.get("integration_pipeline")
                    or ctx.integration_pipeline
                    or "span"
                )
                self._recorded_span_name = name
                _enqueue_lifecycle(
                    record_span_started,
                    name=name,
                    correlation=corr,
                    start_time=self._start_time,
                    attrs=self.overrides.get("attrs"),
                    metadata=self.overrides.get("metadata"),
                )

        # Automatically bind State if possible (to avoid circular imports, we do it carefully)
        try:
            from .integration.state import StateHandle

            handle = StateHandle(ctx)
            self.state_token = _state_handle.set(handle)
        except (ImportError, AttributeError):
            pass

        return ctx

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._is_root_run and self._last_ctx is not None:
            # Dispatch any fire-and-forget task invocations created during this
            # run before the run is recorded as ended.
            from .decorators import flush_pending_invocations

            try:
                flush_pending_invocations(run_id=self._last_ctx.run_id)
            except Exception:
                if exc_type is None:
                    raise
                # The run body is already failing; don't mask its exception.
                import logging

                logging.getLogger(__name__).exception(
                    "Failed to dispatch pending task invocations at run exit"
                )
        if self._record_lifecycle and self._last_ctx is not None:
            from .observability.ingestion import (
                _enqueue_lifecycle,
                record_run_ended,
                record_span_ended,
            )

            corr = self._last_ctx.corelation
            if self._is_root_run:
                status = "FAILED" if exc_type else "SUCCEEDED"
                base_meta = dict(self.overrides.get("metadata") or {})
                base_meta["fw.outcome"] = status
                _enqueue_lifecycle(
                    record_run_ended,
                    status=status,
                    correlation=corr,
                    metadata=base_meta,
                )
            elif self._recorded_span_name:
                status = "ERROR" if exc_type else "OK"
                base_meta = dict(self.overrides.get("metadata") or {})
                base_meta["fw.outcome"] = status
                _enqueue_lifecycle(
                    record_span_ended,
                    name=self._recorded_span_name,
                    status=status,
                    correlation=corr,
                    start_time=self._start_time,
                    end_time=datetime.now(UTC),
                    error_summary=str(exc_val) if exc_val else None,
                    attrs=self.overrides.get("attrs"),
                    metadata=base_meta,
                )
        if self.state_token:
            _state_handle.reset(self.state_token)
        if self.token:
            reset_context(self.token)

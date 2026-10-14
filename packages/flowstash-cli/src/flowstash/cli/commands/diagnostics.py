"""Resource-specific read-only diagnostics. No payload hydration or page draining."""
from enum import Enum
from typing import Optional
import typer
from ..core.diagnostics import DiagnosticCommand, DiagnosticGroup, execute, resolve_context, query_options

class Order(str, Enum):
    asc = "asc"
    desc = "desc"

class EventType(str, Enum):
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    ENDED = "ENDED"
    SCHEDULED = "SCHEDULED"

class BuildSeverity(str, Enum):
    DEFAULT = "DEFAULT"
    DEBUG = "DEBUG"
    INFO = "INFO"
    NOTICE = "NOTICE"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    ALERT = "ALERT"
    EMERGENCY = "EMERGENCY"

obs_app = typer.Typer(help="Read managed execution evidence", cls=DiagnosticGroup, no_args_is_help=True)
builds_app = typer.Typer(help="Read managed builds and paginated build logs", cls=DiagnosticGroup, no_args_is_help=True)


def observation(resource, options):
    options = dict(options)
    json_mode = options.pop("json_mode")
    project, env, user = (options.pop(k) for k in ("project", "env", "user"))
    run_id = options.get("run_id")
    async def work():
        params, window = query_options(options, scoped=bool(run_id), logs=resource == "logs", default_window=not run_id)
        context = await resolve_context(project, env, user)
        result = await context.api.diagnostic_query(resource, {**context.location, **params}, run_id if resource != "runs" else None)
        return result, window
    execute(work, json_mode)


@obs_app.command("run", cls=DiagnosticCommand)
def run_summary(
    run_id: str = typer.Argument(...),
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
):
    """Read one run summary in the selected workspace."""
    async def work():
        context = await resolve_context(project, env, user)
        result = await context.api.diagnostic_run(run_id, context.location)
        context.check_item(result)
        return result, {}
    execute(work, json_mode)


@obs_app.command("runs", cls=DiagnosticCommand)
def runs(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    status: Optional[str] = typer.Option(None, "--status", help='status'),
):
    """Read one page of runs; fetch additional pages explicitly."""
    observation("runs", locals())


@obs_app.command("logs", cls=DiagnosticCommand)
def logs(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    severity: Optional[str] = typer.Option(None, "--severity", help='severity'),
    search: Optional[str] = typer.Option(None, "--search", help='search'),
):
    """Read one page of logs; fetch additional pages explicitly."""
    observation("logs", locals())


@obs_app.command("spans", cls=DiagnosticCommand)
def spans(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    status: Optional[str] = typer.Option(None, "--status", help='status'),
    search: Optional[str] = typer.Option(None, "--search", help='search'),
):
    """Read one page of spans; fetch additional pages explicitly."""
    observation("spans", locals())


@obs_app.command("io", cls=DiagnosticCommand)
def io(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    state: Optional[str] = typer.Option(None, "--state", help='state'),
    remote_system: Optional[str] = typer.Option(None, "--remote-system", help='remote system'),
    channel: Optional[str] = typer.Option(None, "--channel", help='channel'),
    operation: Optional[str] = typer.Option(None, "--operation", help='operation'),
    status_code: Optional[int] = typer.Option(None, "--status-code", help='status code'),
    query: Optional[str] = typer.Option(None, "--query", help='query'),
):
    """Read one page of io; fetch additional pages explicitly."""
    observation("data-exchanges", locals())


@obs_app.command("records", cls=DiagnosticCommand)
def records(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    kind: Optional[str] = typer.Option(None, "--kind", help='kind'),
    source: Optional[str] = typer.Option(None, "--source", help='source'),
):
    """Read one page of records; fetch additional pages explicitly."""
    observation("records", locals())


@obs_app.command("events", cls=DiagnosticCommand)
def events(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    integration: Optional[str] = typer.Option(None, "--integration", help='Exact integration'),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", help='Exact pipeline'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Exact run id'),
    span_id: Optional[str] = typer.Option(None, "--span-id", help='Exact span id'),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help='Exact trace id'),
    parent_run_id: Optional[str] = typer.Option(None, "--parent-run-id", help='Exact parent run id'),
    operation_id: Optional[str] = typer.Option(None, "--operation-id", help='Exact operation id'),
    record_key: Optional[str] = typer.Option(None, "--record-key", help='Exact record key'),
    status: Optional[str] = typer.Option(None, "--status", help='status'),
    event_type: Optional[EventType] = typer.Option(None, "--event-type", help='event type'),
):
    """Read one page of events; fetch additional pages explicitly."""
    observation("run-events", locals())


@obs_app.command("payload", cls=DiagnosticCommand)
def payload(
    payload_ref: str = typer.Argument(...),
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    run_id: Optional[str] = typer.Option(None, "--run-id", help='Captured owning run'),
):
    """Fetch one stored payload; binary data remains base64 in JSON."""
    async def work():
        context = await resolve_context(project, env, user)
        params = {**context.location}
        if run_id is not None:
            params["run_id"] = run_id
        return await context.api.diagnostic_payload(payload_ref, params), {}
    execute(work, json_mode)


@builds_app.command("get", cls=DiagnosticCommand)
def build_get(
    build_id: str = typer.Argument(...),
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
):
    """Read status using a managed build ID; does not trigger a build."""
    async def work():
        context = await resolve_context(project, env, user)
        result = await context.api.build_status(build_id)
        context.check_item(result, environment=False)
        return result, {}
    execute(work, json_mode)


@builds_app.command("list", cls=DiagnosticCommand)
def build_list(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
):
    """Read one page of environment-neutral builds (server time/order semantics)."""
    options = locals().copy()
    async def work():
        filters = {k: options[k] for k in ("limit", "page_token", "order", "from_time", "to_time", "since")}
        params, window = query_options(filters, scoped=True)
        # The native build-list API has fixed ordering, no environment filter,
        # and optional epoch bounds rather than observability's 24-hour default.
        if order is not None and order != Order.desc:
            from ..core.diagnostics import DiagnosticError
            raise DiagnosticError("INVALID_ORDER", "Build list uses server descending order; use --order desc")
        params.pop("order")
        params["page_size"] = params.pop("limit")
        from ..core.diagnostics import timestamp
        for old, new in (("from_time", "start_ts"), ("to_time", "end_ts")):
            if old in params:
                params[new] = timestamp(params.pop(old)).timestamp()
        context = await resolve_context(project, env, user)
        params["project_id"] = context.info["project_id"]
        return await context.api.get("/v1/builds", params=params), window
    execute(work, json_mode)


@builds_app.command("logs", cls=DiagnosticCommand)
def build_logs(
    build_id: str = typer.Argument(...),
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
    limit: Optional[int] = typer.Option(None, "--limit", help='Page size: 50; run logs: 100 (maximum 500)', min=1, max=500),
    page_token: Optional[str] = typer.Option(None, "--page-token", help='Opaque continuation token; reuse fixed times and filters'),
    order: Optional[Order] = typer.Option(None, "--order", help='Project queries: desc; run/build logs: asc'),
    from_time: Optional[str] = typer.Option(None, "--from", help='Inclusive timezone-aware RFC3339 start'),
    to_time: Optional[str] = typer.Option(None, "--to", help='Timezone-aware RFC3339 end'),
    since: Optional[str] = typer.Option(None, "--since", help='Positive duration: 30m, 2h, 7d'),
    severity: Optional[BuildSeverity] = typer.Option(None, "--severity", help='Cloud Logging severity'),
    search: Optional[str] = typer.Option(None, "--search", help='Cloud Logging quoted text search'),
):
    """Read one page of all build log entries, including preamble."""
    options = locals().copy()
    async def work():
        filters = {k: options[k] for k in ("limit", "page_token", "order", "from_time", "to_time", "since", "severity", "search")}
        params, window = query_options(filters, scoped=True)
        context = await resolve_context(project, env, user)
        status = await context.api.build_status(build_id)
        context.check_item(status, environment=False)
        return await context.api.build_log_entries(build_id, params), window
    execute(work, json_mode)


def context_command(
    json_mode: bool = typer.Option(False, "--json", help='Emit the unchanged server JSON'),
    project: Optional[str] = typer.Option(None, "--project", help='Managed project ID'),
    env: Optional[str] = typer.Option(None, "--env", help='Environment (default: prod)'),
    user: Optional[str] = typer.Option(None, "--user", help='Login account; cannot combine with FLOWSTASH_API_KEY'),
):
    """Resolve and validate the current managed project and authentication."""
    async def work():
        context = await resolve_context(project, env, user, require_managed=False)
        return context.info, {}
    execute(work, json_mode)

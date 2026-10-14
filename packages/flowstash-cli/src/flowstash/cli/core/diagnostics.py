"""Read-only managed context, fixed query windows, and machine-safe output."""
import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import click
import httpx
import typer
from typer.core import TyperCommand, TyperGroup

from .api_client import APIClient
from .config import find_project_root, load_project_config, resolve_credentials


class DiagnosticError(Exception):
    def __init__(self, code, message, exit_code=2, http_status=None):
        self.code, self.message = code, message
        self.exit_code, self.http_status = exit_code, http_status


def fail(error, json_mode):
    detail = {"code": error.code, "message": error.message}
    if error.http_status is not None:
        detail["http_status"] = error.http_status
    typer.echo(json.dumps({"error": detail}) if json_mode else error.message, err=True)
    raise typer.Exit(error.exit_code)


class MachineParsing:
    """Keep Click's own parameter/unknown-option errors machine readable too."""
    def parse_args(self, ctx, args):
        json_mode = "--json" in args
        try:
            return super().parse_args(ctx, args)
        except click.ClickException as exc:
            fail(DiagnosticError("INVALID_PARAMETERS", exc.format_message()), json_mode)


class DiagnosticCommand(MachineParsing, TyperCommand):
    pass


class DiagnosticGroup(MachineParsing, TyperGroup):
    def resolve_command(self, ctx, args):
        try:
            return super().resolve_command(ctx, args)
        except click.ClickException as exc:
            fail(DiagnosticError("INVALID_PARAMETERS", exc.format_message()), "--json" in args)


@dataclass
class ManagedContext:
    api: APIClient
    info: dict

    @property
    def location(self):
        return {"project_id": self.info["project_id"], "environment": self.info["environment"]}

    def check_item(self, item, *, environment=True):
        # Single-item backend routes may not accept workspace query filters.
        if item.get("project_id") != self.info["project_id"] or (
            environment and item.get("environment") != self.info["environment"]
        ):
            raise DiagnosticError("NOT_FOUND", "Item is outside the selected project/environment", 4, 404)


async def resolve_context(project=None, env=None, user=None, *, require_managed=True):
    root = find_project_root()
    try:
        local = load_project_config(root, strict=True) if root else None
    except ValueError as exc:
        raise DiagnosticError("INVALID_CONTEXT", str(exc)) from None
    project_id = project or (local.project_id if local else None)
    if not project_id:
        raise DiagnosticError("PROJECT_REQUIRED", "Link a .flowstash project or supply --project PROJECT_ID")
    environment = env or "prod"
    managed = True
    if local and local.environments:
        mode = next((e for e in local.environments if e.name == environment), None)
        if mode is None:
            raise DiagnosticError("UNKNOWN_ENVIRONMENT", f"Unknown environment '{environment}'; configured: {', '.join(e.name for e in local.environments)}")
        managed = mode.managed
        if require_managed and not managed:
            raise DiagnosticError("LOCAL_ENVIRONMENT", f"Environment '{environment}' is local; diagnostics require a managed --env")
    key = os.getenv("FLOWSTASH_API_KEY")
    if key and user:
        raise DiagnosticError("AMBIGUOUS_IDENTITY", "Do not combine --user with FLOWSTASH_API_KEY")
    token = key or resolve_credentials(user=user, project_root=root)
    if not token:
        message = f"No token for requested user '{user}'" if user else "Run flowstash login or configure FLOWSTASH_API_KEY"
        raise DiagnosticError("AUTH_REQUIRED", message, 3, 401)
    api = APIClient(token=token)
    identity = await api.get("/v1/auth/me")
    requested_user = user or (os.getenv("FLOWSTASH_USER") if not key else None)
    if requested_user and identity.get("email") != requested_user:
        raise DiagnosticError("IDENTITY_MISMATCH", "Authenticated identity differs from the requested user; log in again for that user", 3, 401)
    tenant = identity.get("tenant_id")
    if local and local.tenant_id and local.tenant_id != tenant:
        raise DiagnosticError("TENANT_MISMATCH", "Local tenant differs from authenticated tenant; supply the correct --user or FLOWSTASH_API_KEY")
    if identity.get("project_id") and identity["project_id"] != project_id:
        raise DiagnosticError("PROJECT_RESTRICTION", "API key is restricted to another project; select its --project", 3, 403)
    if identity.get("environment") and identity["environment"] != environment:
        raise DiagnosticError("ENVIRONMENT_RESTRICTION", f"API key requires --env {identity['environment']}", 3, 403)
    remote = await api.get(f"/v1/projects/{quote(project_id, safe='')}")
    if remote.get("tenant_id") != tenant or remote.get("project_id") != project_id:
        raise DiagnosticError("TENANT_MISMATCH", "Remote project does not match authenticated tenant/project", 3, 403)
    return ManagedContext(api, {
        "project_root": str(root) if root else None,
        "project_id": project_id, "project_name": remote.get("name"),
        "tenant_id": tenant, "environment": environment, "managed": managed,
        "api_url": api.base_url, "auth_method": "api_key" if key else "login",
        "user_email": identity.get("email"),
        "project_source": "explicit" if project else "local_config",
    })


def timestamp(value):
    if value is None:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})", value):
        raise DiagnosticError("INVALID_TIME", "Timestamps must be timezone-aware RFC3339 values")
    try:
        return datetime.fromisoformat(value.upper().replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        raise DiagnosticError("INVALID_TIME", "Invalid RFC3339 timestamp") from None


def time_window(from_time=None, to_time=None, since=None, *, default_window=False, page_token=None):
    start, end = timestamp(from_time), timestamp(to_time)
    duration = None
    if since is not None:
        if from_time is not None:
            raise DiagnosticError("INVALID_TIME", "--since cannot be combined with --from")
        match = re.fullmatch(r"([1-9][0-9]*)([smhd])", since)
        if not match:
            raise DiagnosticError("INVALID_TIME", "--since requires a positive integer duration, e.g. 30m, 2h, 7d")
        try:
            duration = timedelta(seconds=int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]])
        except (OverflowError, ValueError):
            raise DiagnosticError("INVALID_TIME", "--since duration is too large") from None
    # Never manufacture different timestamps for a continuation. An opaque cursor
    # cannot tell us the exact timestamps sent by an earlier CLI process.
    if page_token and (since or (default_window and (start is None or end is None))):
        raise DiagnosticError("FIXED_WINDOW_REQUIRED", "For pagination reuse the exact --from and --to from the first page; do not use --since")
    try:
        if default_window or duration:
            end = end or datetime.now(timezone.utc)
            start = start or end - (duration or timedelta(hours=24))
    except OverflowError:
        raise DiagnosticError("INVALID_TIME", "Time range is outside supported dates") from None
    if start and end and start >= end:
        raise DiagnosticError("INVALID_TIME", "--from must be before --to")
    return {key: value.isoformat().replace("+00:00", "Z") for key, value in (("from_time", start), ("to_time", end)) if value is not None}


def query_options(options, *, scoped=False, logs=False, default_window=False):
    options = dict(options)
    window = time_window(options.pop("from_time", None), options.pop("to_time", None), options.pop("since", None), default_window=default_window, page_token=options.get("page_token"))
    limit = options.get("limit")
    options["limit"] = limit if limit is not None else (100 if logs and scoped else 50)
    options["order"] = options.get("order") or ("asc" if scoped else "desc")
    return {k: v for k, v in {**options, **window}.items() if v is not None}, window


def execute(work, json_mode=False):
    try:
        result, window = asyncio.run(work())
    except DiagnosticError as exc:
        fail(exc, json_mode)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        codes = {400: ("INVALID_QUERY", 2), 401: ("AUTH_REQUIRED", 3), 403: ("FORBIDDEN", 3), 404: ("NOT_FOUND", 4), 422: ("INVALID_PARAMETERS", 2)}
        code, exit_code = codes.get(status, ("PROVIDER_FAILURE", 5))
        # Do not print request URLs/headers or arbitrary provider response bodies:
        # they can contain credentials, cursor values, and untrusted content.
        messages = {400: "Invalid filter or cursor; start a fresh search if filters changed", 401: "Run flowstash login or configure FLOWSTASH_API_KEY", 403: "Access denied; check observability:read scope and project/environment restrictions", 404: "Item missing or inaccessible; recent evidence may still be ingesting", 422: "Invalid request parameters", 502: "Provider or storage unavailable"}
        fail(DiagnosticError(code, messages.get(status, "Managed API request failed"), exit_code, status), json_mode)
    except httpx.RequestError:
        fail(DiagnosticError("NETWORK_FAILURE", "Unable to reach the managed API", 5), json_mode)
    except Exception:
        fail(DiagnosticError("CLIENT_FAILURE", "Unable to read diagnostics; check configuration and credential storage", 5), json_mode)
    if not json_mode:
        if window:
            typer.echo("Time range: " + " ".join(f"--{k.replace('_time', '')} {v}" for k, v in window.items()))
        if isinstance(result, dict) and result.get("next_page_token"):
            typer.echo(f"Next page: --page-token {result['next_page_token']} (reuse the exact time range and filters)")
    typer.echo(json.dumps(result, ensure_ascii=False, indent=None if json_mode else 2))

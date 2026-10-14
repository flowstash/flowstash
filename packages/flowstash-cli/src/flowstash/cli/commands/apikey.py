"""
API key management commands.

Available as both:
  flowstash api-keys <cmd>          (top-level shortcut)
  flowstash project apikey <cmd>    (legacy / project-scoped path)

Commands:
  new     — Create a new API key
  list    — List all active API keys
  revoke  — Revoke an API key
"""

from typing import Optional
from datetime import datetime
from pathlib import Path
import asyncio
import re

import typer
import questionary
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from ..core.api_client import APIClient
from ..core.config import get_access_token, resolve_credentials, find_project_root, load_project_config

app = typer.Typer(
    name="api-keys",
    help="Manage observability API keys",
    no_args_is_help=True,
)
console = Console()

_VALID_SCOPES = {"observability:ingest", "observability:read", "admin"}
_SCOPE_DESCRIPTIONS = {
    "observability:ingest": "Write-only ingestion (SDK / workers)",
    "observability:read": "Read-only managed diagnostics (agents)",
    "admin": "Full management access (CI / automation)",
}


def _format_ts(ts) -> str:
    if not ts:
        return "—"
    try:
        return datetime.utcfromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


def _find_project_root(start_path: Optional[Path] = None) -> Optional[Path]:
    start_path = start_path or Path.cwd()
    for parent in [start_path] + list(start_path.parents):
        if (parent / ".flowstash").exists() or (parent / "pyproject.toml").exists():
            return parent
    return None


def _write_key_to_env(env_name: str, raw_key: str) -> None:
    """Write FLOWSTASH_API_KEY to the environment's .env file."""
    root = _find_project_root()
    if not root:
        console.print(
            "[yellow]Could not find project root — skipping .env update.[/yellow]"
        )
        return

    candidates = [
        root / "_config" / env_name / ".env",
        root / env_name / ".env",
    ]
    env_file = next((p for p in candidates if p.exists()), None)

    if not env_file:
        env_dir = root / env_name
        if env_dir.is_dir():
            found = list(env_dir.glob("**/.env"))
            if found:
                env_file = found[0]

    if not env_file:
        console.print(
            f"[yellow]Could not find .env for environment '{env_name}'. "
            "Set FLOWSTASH_API_KEY manually.[/yellow]"
        )
        return

    content = env_file.read_text()
    new_line = f"FLOWSTASH_API_KEY={raw_key}"

    if "FLOWSTASH_API_KEY=" in content:
        content = re.sub(r"#?\s*FLOWSTASH_API_KEY=.*", new_line, content)
    else:
        content = content.rstrip("\n") + f"\n{new_line}\n"

    env_file.write_text(content)
    console.print(
        f"[green]Written FLOWSTASH_API_KEY to {env_file.relative_to(root)}[/green]"
    )


def _create_key(
    label: Optional[str],
    scope: Optional[str],
    env: Optional[str],
    user: Optional[str] = None,
    project: Optional[str] = None,
    write_env: Optional[str] = None,
):
    """Shared implementation for 'new' and 'create'."""
    token = resolve_credentials(user=user)
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    if not label:
        label = Prompt.ask("Key label", default="observability key")

    if not scope:
        scope = questionary.select(
            "Key scope:",
            choices=[
                questionary.Choice(f"{s}  —  {_SCOPE_DESCRIPTIONS[s]}", s)
                for s in sorted(_VALID_SCOPES - {"observability:read"})
            ],
            default="observability:ingest",
        ).ask()
        if not scope:
            raise typer.Exit(code=1)

    if scope not in _VALID_SCOPES:
        console.print(
            f"[red]Invalid scope '{scope}'. Valid: {', '.join(_VALID_SCOPES)}[/red]"
        )
        raise typer.Exit(code=1)

    # Preserve legacy ingest/admin --env file setup without --project.
    # Read keys require explicit scope and never write to a file implicitly.
    restricted_env = env if project or scope == "observability:read" else None
    if restricted_env and not project:
        root = find_project_root()
        try:
            config = load_project_config(root, strict=True) if root else None
        except ValueError:
            console.print("[red]Malformed .flowstash configuration.[/red]")
            raise typer.Exit(code=2)
        project = config.project_id if config else None
        if not project:
            console.print("[red]An environment restriction requires --project or a linked project.[/red]")
            raise typer.Exit(code=2)

    if scope == "admin" and env:
        console.print(
            "[yellow]Warning: writing an 'admin' key to .env is not recommended. "
            "Admin keys grant full management access — use them in secure CI environments only.[/yellow]"
        )

    async def _create():
        api = APIClient(token=token)
        body = {"label": label, "scopes": [scope]}
        if project:
            body["project_id"] = project
        if restricted_env:
            body["environment"] = restricted_env
        return await api.post("/v1/api-keys", json=body)

    try:
        result = asyncio.run(_create())
    except Exception as e:
        console.print(f"[red]Failed to create API key: {e}[/red]")
        raise typer.Exit(code=1)

    raw_key = result["api_key"]

    console.print()
    console.print("[green]✅ API key created![/green]")
    console.print(f"   Key ID  : [bold]{result['key_id']}[/bold]")
    console.print(f"   Label   : {label}")
    console.print(f"   Scope   : {scope}")
    if project:
        console.print(f"   Project : {project}")
    if restricted_env:
        console.print(f"   Env     : {restricted_env}")
    console.print()
    console.print(
        "[bold yellow]⚠  Save this key — it will NOT be shown again:[/bold yellow]"
    )
    console.print(f"\n   [bold cyan]{raw_key}[/bold cyan]\n")

    target_env = write_env or (env if scope != "observability:read" else None)
    if target_env:
        _write_key_to_env(target_env, raw_key)


@app.command("new")
def apikey_new(
    label: Optional[str] = typer.Option(
        None, "--label", "-l", help="Human-readable label for the key"
    ),
    scope: Optional[str] = typer.Option(
        None, "--scope", "-s", help="Scope: observability:ingest | observability:read | admin"
    ),
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Restrict read keys to an environment; legacy ingest/admin .env setup"
    ),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
    project: Optional[str] = typer.Option(None, "--project", help="Restrict key to a managed project"),
    write_env: Optional[str] = typer.Option(None, "--write-env", help="Explicitly write key to an environment .env file"),
):
    """Create a new API key and optionally write it to an environment .env file."""
    _create_key(label=label, scope=scope, env=env, user=user, project=project, write_env=write_env)


@app.command("create")
def apikey_create(
    label: Optional[str] = typer.Option(
        None, "--label", "-l", help="Human-readable label for the key"
    ),
    scope: Optional[str] = typer.Option(
        None, "--scope", "-s", help="Scope: observability:ingest | observability:read | admin"
    ),
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Restrict read keys to an environment; legacy ingest/admin .env setup"
    ),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
    project: Optional[str] = typer.Option(None, "--project", help="Restrict key to a managed project"),
    write_env: Optional[str] = typer.Option(None, "--write-env", help="Explicitly write key to an environment .env file"),
):
    """Create a new API key (alias for 'new')."""
    _create_key(label=label, scope=scope, env=env, user=user, project=project, write_env=write_env)


@app.command("list")
def apikey_list(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """List all active API keys for the current tenant."""
    token = resolve_credentials(user=user)
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    async def _list():
        api = APIClient(token=token)
        return await api.get("/v1/api-keys")

    try:
        result = asyncio.run(_list())
    except Exception as e:
        console.print(f"[red]Failed to list API keys: {e}[/red]")
        raise typer.Exit(code=1)

    keys = result.get("api_keys", [])
    if not keys:
        console.print("[dim]No active API keys found.[/dim]")
        return

    table = Table(title="API Keys", show_lines=True)
    table.add_column("Key ID", style="bold")
    table.add_column("Label")
    table.add_column("Prefix")
    table.add_column("Scopes", style="cyan")
    table.add_column("Project")
    table.add_column("Environment")
    table.add_column("Created")
    table.add_column("Last Used")

    for k in keys:
        table.add_row(
            k["key_id"],
            k["label"],
            k["key_prefix"],
            ", ".join(k.get("scopes", [])),
            k.get("project_id") or "—",
            k.get("environment") or "—",
            _format_ts(k.get("created_at")),
            _format_ts(k.get("last_used_at")),
        )

    console.print(table)


@app.command("revoke")
def apikey_revoke(
    key_id: str = typer.Argument(..., help="Key ID to revoke (e.g. key-a3b2c1d4)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """Revoke an API key immediately."""
    token = resolve_credentials(user=user)
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    if not yes:
        from rich.prompt import Confirm

        if not Confirm.ask(
            f"Revoke key [bold red]{key_id}[/bold red]? This cannot be undone."
        ):
            console.print("Cancelled.")
            return

    async def _revoke():
        api = APIClient(token=token)
        return await api.delete(f"/v1/api-keys/{key_id}")

    try:
        asyncio.run(_revoke())
    except Exception as e:
        console.print(f"[red]Failed to revoke API key: {e}[/red]")
        raise typer.Exit(code=1)

    console.print(f"[green]Key {key_id} revoked successfully.[/green]")

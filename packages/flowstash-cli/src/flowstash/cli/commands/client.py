"""
Client command group for the flowstash CLI.

Commands:
  list   — List all HTTP clients configured for an environment
  curl   — Send an HTTP request via a configured client (relative path only)

Auth is handled transparently by HttpClient (OAuth2, API Key, Basic).
Custom HttpClient subclasses are loaded by importing src/shared/clients/ modules
to trigger @client(...) decorator registrations.  If that import step fails for
any reason, a plain HttpClient is used as a fallback.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json as json_lib
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import typer
from rich.console import Console
from rich.table import Table

from ..core.config import load_project_config
from .project import find_project_root

app = typer.Typer(
    name="client",
    help="Inspect and interact with configured HTTP clients",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_env(env_arg: Optional[str], project_root: Path) -> str:
    """
    Return the environment name to use.

    Rules:
    - If *env_arg* is supplied and valid → return it.
    - 1 env configured → auto-select (with a dim notice).
    - 0 envs configured → fall back to "shared".
    - >1 envs and no *env_arg* → error; list options and exit.
    """
    project_config = load_project_config()
    envs: List[str] = (
        [e.name for e in project_config.environments] if project_config else []
    )

    if env_arg is not None:
        if envs and env_arg not in envs:
            console.print(
                f"[red]Environment '{env_arg}' not found.[/red] "
                f"Available: {', '.join(envs)}"
            )
            raise typer.Exit(code=1)
        return env_arg

    if len(envs) == 0:
        console.print(
            "[dim]No environments configured — loading shared config.[/dim]"
        )
        return "shared"

    if len(envs) == 1:
        console.print(f"[dim]Using environment: [bold]{envs[0]}[/bold][/dim]")
        return envs[0]

    # Multiple envs: --env is required
    console.print(
        "[red]Multiple environments configured. Specify one with "
        "[bold]--env[/bold] / [bold]-e[/bold]:[/red]"
    )
    for name in envs:
        console.print(f"  [yellow]{name}[/yellow]")
    raise typer.Exit(code=1)


def _load_runtime_config(project_root: Path, env_name: str):
    """Load RuntimeConfig from <project_root>/config for the given environment."""
    config_dir = project_root / "config"
    if not config_dir.is_dir():
        console.print(
            f"[red]No config/ directory found at {project_root}.[/red] "
            "Run [bold]flowstash init[/bold] to set up the project."
        )
        raise typer.Exit(code=1)

    try:
        from flowstash.config.env_loader import load_config_dir

        return load_config_dir(config_dir, env_name)
    except Exception as exc:
        console.print(f"[red]Failed to load config for env '{env_name}': {exc}[/red]")
        raise typer.Exit(code=1)


def _import_client_modules(project_root: Path) -> List[str]:
    """
    Import every *.py file under src/shared/clients/ (excluding __init__.py)
    so that @client(...) decorators fire and register subclasses.

    Returns a list of successfully imported module paths (for diagnostics).
    Falls back silently on any per-file ImportError.
    """
    clients_dir = project_root / "src" / "shared" / "clients"
    if not clients_dir.is_dir():
        return []

    # Add project root and src/ to sys.path so relative imports work
    extra_paths = [str(project_root), str(project_root / "src")]
    added: List[str] = []
    for p in extra_paths:
        if p not in sys.path:
            sys.path.insert(0, p)
            added.append(p)

    imported: List[str] = []
    try:
        for py_file in sorted(clients_dir.glob("*.py")):
            if py_file.name.startswith("_"):
                continue
            module_name = f"shared.clients.{py_file.stem}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    sys.modules.setdefault(module_name, mod)
                    spec.loader.exec_module(mod)  # type: ignore[union-attr]
                    imported.append(module_name)
            except Exception as exc:  # noqa: BLE001
                console.print(
                    f"[dim yellow]Warning: could not import {py_file.name}: {exc}[/dim yellow]"
                )
    finally:
        # Restore sys.path
        for p in added:
            try:
                sys.path.remove(p)
            except ValueError:
                pass

    return imported


def _get_http_client(client_id: str, config, project_root: Path) -> Any:
    """
    Return an HttpClient (or custom subclass) for *client_id*.

    Strategy:
    1. Import user client modules from src/shared/clients/ to trigger @client() registrations.
    2. init_registry(config) → flush deferred registrations into ClientRegistry.
    3. ClientRegistry.get_client(client_id) → returns custom class if registered, else HttpClient.
    4. On any failure, fall back to plain HttpClient(name, settings).
    """
    settings = config.clients.get(client_id)
    if settings is None:
        raise KeyError(client_id)

    try:
        from flowstash.clients.registry import ClientRegistry, init_registry

        _import_client_modules(project_root)
        init_registry(config)
        return ClientRegistry.get_instance().get_client(client_id)

    except Exception as exc:  # noqa: BLE001
        console.print(
            f"[dim yellow]Warning: registry init failed ({exc}); "
            "using plain HttpClient.[/dim yellow]"
        )
        from flowstash.clients.http import HttpClient

        return HttpClient(name=client_id, settings=settings)


# ---------------------------------------------------------------------------
# Async request helper
# ---------------------------------------------------------------------------


async def _do_request(
    http_client,
    method: str,
    path: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    json_body: Optional[Any],
    data_body: Optional[str],
) -> httpx.Response:
    return await http_client.request(
        method.upper(),
        path,
        headers=headers or None,
        params=params or None,
        json=json_body,
        data=data_body,
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _discover_envs(project_root: Path) -> List[str]:
    """
    Return sorted environment names by listing subdirectories of config/
    that are not 'shared'.
    """
    config_dir = project_root / "config"
    if not config_dir.is_dir():
        return []
    return sorted(
        d.name
        for d in config_dir.iterdir()
        if d.is_dir() and d.name != "shared"
    )


def _print_clients_table(env_name: str, config) -> None:
    if not config.clients:
        console.print(f"[yellow]  (no clients for '{env_name}')[/yellow]")
        return

    table = Table(
        title=f"[bold]{env_name}[/bold]",
        show_lines=False,
        header_style="bold cyan",
        title_justify="left",
    )
    table.add_column("Client ID", style="bold")
    table.add_column("Base URL")

    for client_id, settings in sorted(config.clients.items()):
        table.add_row(client_id, settings.base_url)

    console.print(table)


@app.command("list")
def list_clients(
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Show clients for a specific environment only"
    ),
):
    """
    List configured HTTP clients.

    Without [bold]--env[/bold], shows clients for every environment found in
    the [bold]config/[/bold] directory.  Pass [bold]--env[/bold] to narrow
    output to a single environment.
    """
    project_root = find_project_root()
    if not project_root:
        console.print("[red]Not in a flowstash project.[/red]")
        raise typer.Exit(code=1)

    if env is not None:
        # Single-env mode: validate and show just that env
        available = _discover_envs(project_root)
        if available and env not in available:
            console.print(
                f"[red]Environment '{env}' not found.[/red] "
                f"Available: {', '.join(available)}"
            )
            raise typer.Exit(code=1)
        config = _load_runtime_config(project_root, env)
        _print_clients_table(env, config)
        return

    # All-envs mode: iterate config/ subdirectories
    envs = _discover_envs(project_root)
    if not envs:
        console.print(
            "[yellow]No environment directories found under config/.[/yellow]"
        )
        return

    for env_name in envs:
        try:
            config = _load_runtime_config(project_root, env_name)
        except SystemExit:
            console.print(
                f"[dim yellow]  Skipping '{env_name}': failed to load config.[/dim yellow]"
            )
            continue
        _print_clients_table(env_name, config)


@app.command("curl")
def curl_request(
    client_id: str = typer.Argument(..., help="Client ID to use (e.g. demoClient)"),
    path: str = typer.Argument(..., help="Relative path (e.g. /users/1)"),
    method: str = typer.Option("GET", "--method", "-X", help="HTTP method"),
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Environment to load config from"
    ),
    header: Optional[List[str]] = typer.Option(
        None,
        "--header",
        "-H",
        help="Extra request header in Name:Value format (repeatable)",
    ),
    data: Optional[str] = typer.Option(
        None, "--data", "-d", help="Raw request body string"
    ),
    json: Optional[str] = typer.Option(
        None,
        "--json",
        help="JSON request body as inline string or @filepath",
    ),
    query: Optional[List[str]] = typer.Option(
        None,
        "--query",
        "-q",
        help="Query parameter in key=value format (repeatable)",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Print request/response headers"
    ),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Write response body to file"
    ),
):
    """
    Send an HTTP request through a configured client.

    Auth (OAuth2, API Key, Basic) is applied automatically from the client's
    configuration. Only a relative path is needed — the base URL comes from
    the client settings.

    \b
    Examples:
      flowstash client curl demoClient /users
      flowstash client curl demoClient /users/1 -X GET -v
      flowstash client curl demoClient /items -X POST --json '{"name":"x"}'
      flowstash client curl demoClient /items -X POST --json @payload.json
      flowstash client curl demoClient /search -q "q=hello" -q "page=1"
    """
    project_root = find_project_root()
    if not project_root:
        console.print("[red]Not in a flowstash project.[/red]")
        raise typer.Exit(code=1)

    env_name = _resolve_env(env, project_root)
    config = _load_runtime_config(project_root, env_name)

    # Validate client_id
    if client_id not in config.clients:
        console.print(
            f"[red]Client '{client_id}' not found in environment '{env_name}'.[/red]"
        )
        if config.clients:
            console.print(
                f"Available clients: {', '.join(sorted(config.clients.keys()))}"
            )
        raise typer.Exit(code=1)

    # Parse headers: "Name:Value" → dict
    extra_headers: Dict[str, str] = {}
    for h in header or []:
        if ":" not in h:
            console.print(
                f"[red]Invalid header '{h}' — expected Name:Value format.[/red]"
            )
            raise typer.Exit(code=1)
        k, v = h.split(":", 1)
        extra_headers[k.strip()] = v.strip()

    # Parse query params: "key=value" → dict
    extra_params: Dict[str, str] = {}
    for q in query or []:
        if "=" not in q:
            console.print(
                f"[red]Invalid query param '{q}' — expected key=value format.[/red]"
            )
            raise typer.Exit(code=1)
        k, v = q.split("=", 1)
        extra_params[k.strip()] = v.strip()

    # Parse JSON body
    json_body: Optional[Any] = None
    if json is not None:
        raw_json: str
        if json.startswith("@"):
            json_file = Path(json[1:])
            if not json_file.exists():
                console.print(f"[red]JSON file not found: {json_file}[/red]")
                raise typer.Exit(code=1)
            raw_json = json_file.read_text(encoding="utf-8")
        else:
            raw_json = json
        try:
            json_body = json_lib.loads(raw_json)
        except json_lib.JSONDecodeError as exc:
            console.print(f"[red]Invalid JSON: {exc}[/red]")
            raise typer.Exit(code=1)

    # Get client (with custom-subclass resolution)
    try:
        http_client = _get_http_client(client_id, config, project_root)
    except KeyError:
        console.print(f"[red]Client '{client_id}' could not be instantiated.[/red]")
        raise typer.Exit(code=1)

    settings = config.clients[client_id]
    full_url = (
        settings.base_url.rstrip("/") + "/" + path.lstrip("/")
        if path not in ("", ".")
        else settings.base_url
    )

    if verbose:
        console.print(f"[dim]> {method.upper()} {full_url}[/dim]")
        for k, v in extra_headers.items():
            console.print(f"[dim]> {k}: {v}[/dim]")
        if extra_params:
            console.print(f"[dim]> params: {extra_params}[/dim]")

    # Execute request
    try:
        response: httpx.Response = asyncio.run(
            _do_request(
                http_client,
                method,
                path,
                extra_headers,
                extra_params,
                json_body,
                data,
            )
        )
    except Exception as exc:
        console.print(f"[red]Request failed: {exc}[/red]")
        raise typer.Exit(code=1)

    # Status line
    status = response.status_code
    if 200 <= status < 300:
        status_style = "bold green"
    elif 300 <= status < 400:
        status_style = "bold yellow"
    else:
        status_style = "bold red"

    console.print(f"[{status_style}]{status} {response.reason_phrase}[/{status_style}]")

    if verbose:
        for k, v in response.headers.items():
            console.print(f"[dim]< {k}: {v}[/dim]")

    # Body output
    body = response.text
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            pretty = json_lib.dumps(json_lib.loads(body), indent=2, ensure_ascii=False)
            console.print_json(pretty)
            body = pretty
        except Exception:
            console.print(body)
    else:
        console.print(body)

    if output:
        out_path = Path(output)
        out_path.write_text(body, encoding="utf-8")
        console.print(f"[dim]Response written to {out_path}[/dim]")

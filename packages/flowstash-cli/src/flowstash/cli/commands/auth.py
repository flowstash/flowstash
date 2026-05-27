from typing import Optional
import typer
import webbrowser
import secrets
from rich.console import Console
from rich.table import Table
import asyncio
import httpx
from ..core.auth_server import start_callback_server, find_available_port
from ..core.api_client import APIClient
from ..core.config import (
    load_global_config,
    get_access_token,
    delete_access_token,
    load_project_config,
    save_project_config,
    register_user,
    resolve_credentials,
    get_token_for_user,
    ProjectConfig,
)

app = typer.Typer()
console = Console()
import os

API_URL = os.getenv("FLOWSTASH_API_URL", "https://api.flowstash.dev")


async def _fetch_user_info(token: str) -> Optional[dict]:
    """Fetch /v1/auth/me using an explicit token (used right after login)."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{API_URL}/v1/auth/me",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception:
        return None


@app.command()
def login(
    username: Optional[str] = typer.Option(
        None, "--username", "-u", help="Username / email for manual login"
    ),
    password: Optional[str] = typer.Option(
        None, "--password", "-p", help="Password for manual login"
    ),
):
    """Log in to the flowstash Managed Platform.

    Multiple accounts are supported — logging in as a new user does NOT
    log out existing sessions.  Use [bold]flowstash accounts[/bold] to see
    all active sessions.
    """
    # ── Existing-account check ──────────────────────────────────────────────
    global_config = load_global_config()
    if global_config.accounts:
        console.print("[bold]You already have logged-in accounts:[/bold]")
        for i, acc in enumerate(global_config.accounts, 1):
            label = (
                f"{acc.display_name} ({acc.email})" if acc.display_name else acc.email
            )
            console.print(f"  {i}. {label}")
        new_idx = len(global_config.accounts) + 1
        console.print(f"  {new_idx}. Login as a new user")

        raw = typer.prompt("Select an option", default=str(new_idx))
        try:
            choice = int(raw)
        except ValueError:
            choice = new_idx

        if 1 <= choice <= len(global_config.accounts):
            selected = global_config.accounts[choice - 1]
            project_config = load_project_config() or ProjectConfig()
            project_config.linked_user = selected.email
            save_project_config(project_config)
            console.print(
                f"[green]Project account set to: [bold]{selected.email}[/bold][/green]"
            )
            return
        # else: fall through to normal login flow

    if username and password:
        console.print(f"Logging in to {API_URL} as {username}...")

        async def do_login():
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.post(
                        f"{API_URL}/v1/auth/login",
                        json={"email": username, "password": password},
                    )
                    resp.raise_for_status()
                    return resp.json()
                except Exception as e:
                    console.print(f"[red]Login failed: {e}[/red]")
                    return None

        result = asyncio.run(do_login())
        if not result:
            raise typer.Exit(code=1)

        access_token = result.get("access_token")
        if not access_token:
            console.print("[red]No access token in response.[/red]")
            raise typer.Exit(code=1)

        # Resolve the canonical email from the API (may differ from what was typed)
        user_info = asyncio.run(_fetch_user_info(access_token))
        email = (
            (user_info.get("email") or user_info.get("username") or username)
            if user_info
            else username
        )
        tenant_id = (user_info or result).get("tenant_id")
        display_name = user_info.get("name") if user_info else None

        register_user(
            email, access_token, display_name=display_name, tenant_id=tenant_id
        )
        console.print(
            f"[green]Logged in as: [bold]{email}[/bold] (tenant: {tenant_id})[/green]"
        )
        return

    # Browser / OAuth login
    state = secrets.token_urlsafe(16)
    try:
        port = find_available_port(8500)
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    callback_url = f"http://localhost:{port}/callback"
    login_url = (
        API_URL.replace("api", "app")
        + f"/cli-login?redirect_uri={callback_url}&state={state}"
    )

    console.print("Opening your browser to authenticate...")
    console.print(
        f"If the browser doesn't open, visit: [link={login_url}]{login_url}[/link]"
    )

    webbrowser.open(login_url)

    console.print("Waiting for authentication...")
    result = start_callback_server(port)

    if not result:
        console.print("[red]Authentication timed out or failed.[/red]")
        raise typer.Exit(code=1)

    if result.get("state") != state:
        console.print(
            "[red]Invalid state received. Auth session might be compromised.[/red]"
        )
        raise typer.Exit(code=1)

    access_token = result.get("access_token")
    if not access_token:
        console.print("[red]No access token received.[/red]")
        raise typer.Exit(code=1)

    user_info = asyncio.run(_fetch_user_info(access_token))
    email = user_info.get("email") or user_info.get("username") if user_info else None
    tenant_id = (user_info or result).get("tenant_id")
    display_name = user_info.get("name") if user_info else None

    if not email:
        print(user_info)
        console.print(
            "[red]Could not determine account email from server response.[/red]"
        )
        raise typer.Exit(code=1)

    register_user(email, access_token, display_name=display_name, tenant_id=tenant_id)
    console.print(
        f"[green]Logged in as: [bold]{email}[/bold] (tenant: {tenant_id})[/green]"
    )


@app.command()
def logout(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account email to log out (default: current user)"
    ),
):
    """Log out from the flowstash Managed Platform.

    Pass [bold]--user[/bold] to log out a specific account while keeping
    other sessions active.
    """
    delete_access_token(email=user)
    label = f" ({user})" if user else ""
    console.print(f"[yellow]Logged out{label} successfully.[/yellow]")


@app.command()
def accounts():
    """List all logged-in accounts."""
    config = load_global_config()
    if not config.accounts:
        console.print("No accounts logged in. Run [bold]flowstash login[/bold] first.")
        return

    table = Table(
        title="Logged-in Accounts", show_header=True, header_style="bold cyan"
    )
    table.add_column("Email")
    table.add_column("Display Name")
    table.add_column("Tenant")
    table.add_column("Active", justify="center")

    for acc in config.accounts:
        is_current = "[green]✓[/green]" if acc.email == config.current_user else ""
        table.add_row(
            acc.email,
            acc.display_name or "[dim]—[/dim]",
            acc.tenant_id or "[dim]—[/dim]",
            is_current,
        )

    console.print(table)


def _fetch_current_user(token: Optional[str] = None) -> Optional[dict]:
    try:
        client = APIClient(token=token) if token else APIClient()
        return asyncio.run(client.get("/v1/auth/me"))
    except Exception:
        return None


@app.command()
def whoami(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Show status for a specific account"
    ),
):
    """Show current login status."""
    token = resolve_credentials(user=user)
    global_config = load_global_config()
    project_config = load_project_config()

    if token:
        console.print(f"API URL: {global_config.api_url}")
        user_info = _fetch_current_user(token=token)
        login = None
        tenant_id = None

        if user_info:
            login = (
                user_info.get("email")
                or user_info.get("username")
                or user_info.get("login")
            )
            tenant_id = user_info.get("tenant_id")
        elif project_config:
            login = project_config.user_email
            tenant_id = project_config.tenant_id

        if login:
            console.print(f"Logged in as: [bold]{login}[/bold]")
        if tenant_id:
            console.print(f"Tenant: [bold]{tenant_id}[/bold]")

        if project_config:
            console.print(f"Current Project: [bold]{project_config.project_id}[/bold]")
            if project_config.linked_user:
                console.print(
                    f"Project Account: [bold]{project_config.linked_user}[/bold]"
                )
        elif not user_info:
            console.print("Logged in, but no project context found in this directory.")

        # Show all sessions summary
        if global_config.accounts and len(global_config.accounts) > 1:
            other = [a.email for a in global_config.accounts if a.email != login]
            console.print(f"[dim]Other active sessions: {', '.join(other)}[/dim]")
    else:
        console.print("Not logged in.")

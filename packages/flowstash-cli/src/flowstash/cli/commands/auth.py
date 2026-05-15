from typing import Optional
import typer
import webbrowser
import uuid
import secrets
from rich.console import Console
import asyncio
import httpx
from ..core.auth_server import start_callback_server
from ..core.api_client import APIClient
from ..core.config import (
    load_global_config, 
    set_access_token, 
    get_access_token, 
    delete_access_token,
    load_project_config
)

app = typer.Typer()
console = Console()
import os
API_URL = os.getenv("FLOWSTASH_API_URL", "https://api.flowstash.dev")

@app.command()
def login(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="Username for manual login"),
    password: Optional[str] = typer.Option(None, "--password", "-p", help="Password for manual login")
):
    """Log in to the flowstash Managed Platform."""
    if username and password:
        # Manual login
        console.print(f"Logging in to {API_URL} as {username}...")
        
        async def do_login():
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.post(
                        f"{API_URL}/v1/auth/login",
                        json={"email": username, "password": password}
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
        tenant_id = result.get("tenant_id")
        
        set_access_token(access_token)
        console.print(f"[green]Successfully logged in as tenant: {tenant_id}[/green]")
        return

    # Browser login
    state = secrets.token_urlsafe(16)
    port = 8888
    callback_url = f"http://localhost:{port}/callback"
    
    login_url = f"{API_URL}/cli-login?redirect_uri={callback_url}&state={state}"
    
    console.print(f"Opening your browser to authenticate...")
    console.print(f"If the browser doesn't open, visit: [link={login_url}]{login_url}[/link]")
    
    webbrowser.open(login_url)
    
    console.print("Waiting for authentication...")
    result = start_callback_server(port)
    
    if not result:
        console.print("[red]Authentication timed out or failed.[/red]")
        raise typer.Exit(code=1)
    
    if result.get("state") != state:
        console.print("[red]Invalid state received. Auth session might be compromised.[/red]")
        raise typer.Exit(code=1)
    
    access_token = result.get("access_token")
    tenant_id = result.get("tenant_id")
    
    if not access_token:
        console.print("[red]No access token received.[/red]")
        raise typer.Exit(code=1)
    
    set_access_token(access_token)
    
    console.print(f"[green]Successfully logged in as tenant: {tenant_id}[/green]")

@app.command()
def logout():
    """Log out from the flowstash Managed Platform."""
    delete_access_token()
    console.print("[yellow]Logged out successfully.[/yellow]")


def _fetch_current_user() -> Optional[dict]:
    try:
        return asyncio.run(APIClient().get("/v1/auth/me"))
    except Exception:
        return None

@app.command()
def whoami():
    """Show current login status."""
    token = get_access_token()
    global_config = load_global_config()
    project_config = load_project_config()
    
    if token:
        console.print(f"API URL: {global_config.api_url}")
        user_info = _fetch_current_user()
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
        elif not user_info:
            console.print("Logged in, but no project context found in this directory.")
    else:
        console.print("Not logged in.")

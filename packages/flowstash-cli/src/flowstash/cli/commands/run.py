import typer
from pathlib import Path
from rich.console import Console
import subprocess
import os
from .project import find_project_root
from ..core.docker_utils import check_docker_binary, check_docker_compose_binary, check_docker_daemon, get_docker_compose_cmd

app = typer.Typer()
console = Console()

@app.command()
def run(
    env: str = typer.Argument(..., help="Environment to run"),
    build: bool = typer.Option(True, "--build/--no-build", help="Build images before starting"),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run in background"),
    logs: bool = typer.Option(False, "--logs", help="Follow logs (useful with --detach)")
):
    """Run the project locally via Docker Compose."""
    root = find_project_root()
    if not root:
        console.print("[red]Not in a flowstash project.[/red]")
        raise typer.Exit(code=1)

    # Docker prerequisites
    if not check_docker_binary():
        console.print("[red]Docker binary not found. Please install Docker.[/red]")
        raise typer.Exit(code=1)
    
    if not check_docker_compose_binary():
        console.print("[red]Docker Compose not found. Please install Docker Compose.[/red]")
        raise typer.Exit(code=1)

    daemon_running, daemon_err = check_docker_daemon()
    if not daemon_running:
        console.print(f"[red]Docker daemon not reachable: {daemon_err}[/red]")
        raise typer.Exit(code=1)

    compose_file = root / "deployment" / env / "docker-compose.yaml"
    if not compose_file.exists():
        console.print(f"[red]No docker-compose.yaml found for env '{env}' at {compose_file}[/red]")
        console.print("[yellow]Try running 'flowstash init --fix' to create it.[/yellow]")
        raise typer.Exit(code=1)

    cmd = get_docker_compose_cmd() + ["-f", str(compose_file), "up"]
    if build:
        cmd.append("--build")
    if detach:
        cmd.append("-d")

    console.print(f"Running flowstash in [bold]{env}[/bold] mode...")
    
    try:
        # We want to stream output directly to terminal
        subprocess.run(cmd, check=True, cwd=root)
        
        if detach and logs:
            log_cmd = get_docker_compose_cmd() + ["-f", str(compose_file), "logs", "-f"]
            subprocess.run(log_cmd, check=True, cwd=root)
            
    except subprocess.CalledProcessError:
        console.print("[red]Docker Compose command failed.[/red]")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping...[/yellow]")

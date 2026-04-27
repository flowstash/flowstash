import typer
import asyncio
import time
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from ..core.api_client import APIClient
from ..core.config import load_project_config
from ..core.builder import bundle_source

import subprocess

app = typer.Typer()
console = Console()

from .project import find_project_root
from ..core.docker_utils import check_docker_binary, check_docker_daemon, get_docker_compose_cmd

async def run_managed_build(tag: str = "latest"):
    project_config = load_project_config()
    if not project_config or not project_config.project_id:
        console.print("[red]Project not linked. Run 'flowstash init' to link to a managed project.[/red]")
        raise typer.Exit(code=1)
    
    api = APIClient()
    # ... rest of existing managed build logic ...
    # (I'll keep the existing implementation but wrap it)

@app.command()
def build(
    env: str = typer.Argument(..., help="Environment to build"),
    tag: str = typer.Option("latest", "--tag", "-t", help="Tag for the image")
):
    """Build project artifacts/images for the specified environment."""
    project_config = load_project_config()
    
    # Check if env is managed
    is_managed = False
    if project_config:
        for em in project_config.environments:
            if em.name == env:
                is_managed = em.managed
                break
    
    if is_managed:
        result = asyncio.run(run_build_flow(tag))
        console.print(f"[green]Managed build completed successfully![/green]")
        console.print(f"Artifact ID: [bold]{result['artifact_id']}[/bold]")
    else:
        # Local build
        root = find_project_root()
        if not root:
            console.print("[red]Not in a flowstash project.[/red]")
            raise typer.Exit(code=1)

        if not check_docker_binary():
            console.print("[red]Docker not found.[/red]")
            raise typer.Exit(code=1)

        compose_file = root / "deployment" / env / "docker-compose.yaml"
        if not compose_file.exists():
            console.print(f"[red]No docker-compose.yaml found for env '{env}' at {compose_file}[/red]")
            raise typer.Exit(code=1)

        cmd = get_docker_compose_cmd() + ["-f", str(compose_file), "build"]
        console.print(f"Building images for [bold]{env}[/bold]...")
        try:
            subprocess.run(cmd, check=True, cwd=root)
            console.print(f"[green]Local build for '{env}' complete![/green]")
        except subprocess.CalledProcessError:
            console.print("[red]Local build failed.[/red]")
            raise typer.Exit(code=1)

async def run_build_flow(tag: str = "latest"):
    # (Moved existing run_build_flow logic here for completeness in the file)
    project_config = load_project_config()
    project_id = project_config.project_id
    api = APIClient()
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ) as progress:
            task = progress.add_task(description="Bundling source code...", total=None)
            tar_path = bundle_source(Path.cwd())
            progress.update(task, description="Source bundled.")
            
            task = progress.add_task(description="Requesting upload path...", total=None)
            upload_data = await api.get("/v1/builds/upload-path")
            build_id = upload_data["build_id"]
            upload_url = upload_data["upload_url"]
            progress.update(task, description="Upload path received.")
            
            task = progress.add_task(description="Uploading source...", total=None)
            with open(tar_path, "rb") as f:
                data = f.read()
            await api.put_binary(upload_url, data, headers={"Content-Type": "application/gzip"})
            progress.update(task, description="Source uploaded.")
            
            task = progress.add_task(description="Triggering build...", total=None)
            trigger_resp = await api.post(f"/v1/builds/{build_id}/trigger", json={"image_tag": tag})
            
            # update build_id to the triggered true GCP build ID
            build_id = trigger_resp["build_id"]
            progress.update(task, description=f"Build triggered (ID: {build_id}).")
            
            task = progress.add_task(description="Building...", total=None)
            while True:
                status_data = await api.get(f"/v1/builds/{build_id}/status")
                status = status_data["status"]
                if status == "SUCCESS":
                    progress.update(task, description="Build successful!")
                    return status_data
                elif status in ["FAILURE", "INTERNAL_ERROR", "TIMEOUT", "CANCELLED"]:
                    progress.update(task, description=f"[red]Build failed: {status}[/red]")
                    console.print(f"[red]Build ended with status: {status}[/red]")
                    log_url = status_data.get('log_url')
                    if log_url:
                        console.print(f"Check logs here: [link={log_url}]{log_url}[/link]")
                    raise typer.Exit(code=1)
                await asyncio.sleep(5)
    except Exception as e:
        if isinstance(e, typer.Exit):
            raise
        console.print(f"[red]Error during build: {e}[/red]")
        raise typer.Exit(code=1)

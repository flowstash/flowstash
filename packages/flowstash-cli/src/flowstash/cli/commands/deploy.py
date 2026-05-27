from typing import Optional
import asyncio
import typer
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from ..core.api_client import APIClient
from ..core.config import load_project_config, resolve_credentials
from .build import run_build_flow
import flowstash.runtime

app = typer.Typer()
console = Console()

# Status labels shown to the user while polling
_STATUS_LABELS = {
    "QUEUED": "Queued, waiting for deployment to start...",
    "VALIDATING": "Validating container images...",
    "DEPLOYING": "Deploying services...",
    "HEALTH_CHECK": "Health-checking API and Worker...",
    "SYNCING_SCHEDULES": "Waiting for deployment verification...",
    "DEPLOYED": "Deployed successfully ✓",
    "FAILED": "Deployment failed.",
}

_TERMINAL_STATUSES = {"DEPLOYED", "FAILED"}


async def run_deploy_flow(env: str, artifact_id: Optional[str] = None, user: Optional[str] = None):
    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_id = project_config.project_id
    if not project_id:
        console.print("[red]project_id not found in .flowstash[/red]")
        raise typer.Exit(code=1)

    token = resolve_credentials(user=user)
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    api = APIClient(token=token)

    # 1. If artifact_id is not provided, run build first
    if not artifact_id:
        console.print("No artifact ID provided. Building first...")
        build_result = await run_build_flow(user=user)
        artifact_id = build_result["artifact_id"]
        console.print(f"Build finished. Deploying artifact: [bold]{artifact_id}[/bold]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        # 2. Trigger deployment
        task = progress.add_task(description="Triggering deployment...", total=None)
        deploy_data = await api.post(
            "/v1/deploy",
            json={
                "project_id": project_id,
                "artifact_id": artifact_id,
                "env_vars": {"ENVIRONMENT": env},
                "flowstash_runtime_version": flowstash.runtime.__version__,
            },
        )
        deploy_id = deploy_data["deploy_id"]
        progress.update(task, description=f"Deployment triggered (ID: {deploy_id}).")

        # 3. Poll status until terminal
        last_status = None
        while True:
            status_data = await api.get(f"/v1/deploy/{deploy_id}/status")
            current_status = status_data["status"]

            if current_status != last_status:
                label = _STATUS_LABELS.get(current_status, f"Status: {current_status}")
                progress.update(task, description=label)
                last_status = current_status

            if current_status == "DEPLOYED":
                break
            elif current_status == "FAILED":
                error = status_data.get("error_message", "Unknown error")
                console.print(f"[red]Deployment failed: {error}[/red]")
                raise typer.Exit(code=1)

            await asyncio.sleep(5)

    return status_data


@app.command()
def deploy(
    env: str = typer.Argument(..., help="Environment to deploy to"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", "-a", help="Artifact ID to deploy"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation"),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """Deploy an artifact to the managed platform for a specified environment."""
    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    # Ask for confirmation unless non-interactive is provided
    if not yes:
        from rich.prompt import Confirm

        if not Confirm.ask(f"Are you sure you want to deploy to '{env}'?"):
            console.print("Deployment cancelled.")
            raise typer.Exit(code=0)

    # Check if env is managed
    is_managed = False
    for em in project_config.environments:
        if em.name == env:
            is_managed = em.managed
            break

    if not is_managed:
        console.print(
            f"[red]Environment '{env}' is not managed. Deployment is only supported for managed environments.[/red]"
        )
        console.print(
            "[yellow]Update your .flowstash environments if this is incorrect.[/yellow]"
        )
        raise typer.Exit(code=1)

    project_id = project_config.project_id
    if not project_id:
        if not yes:
            from rich.prompt import Confirm

            if Confirm.ask(
                "Project ID not found. Would you like to link to a managed project now?"
            ):
                from .project import _link_project

                _link_project(project_config, user=user)
                project_id = project_config.project_id

        if not project_id:
            console.print(
                "[red]project_id not found in .flowstash. Use 'flowstash init' or link to a project.[/red]"
            )
            raise typer.Exit(code=1)

    result = asyncio.run(run_deploy_flow(env, artifact, user=user))

    api_url = result.get("api_url", "")
    console.print(f"[green]✓ Deployment complete![/green]")
    console.print(f"  Deployment ID : [bold]{result['deploy_id']}[/bold]")
    if api_url:
        console.print(f"  API URL       : [bold]{api_url}[/bold]")

    scheduled_tasks = result.get("scheduled_tasks")
    if scheduled_tasks:
        console.print("  Scheduled Tasks:")
        for task in scheduled_tasks:
            console.print(
                f"    - [cyan]{task['task_name']}[/cyan] : [yellow]{task['cron']}[/yellow]"
            )
    elif scheduled_tasks is not None:
        console.print("  Scheduled Tasks: [dim]None[/dim]")

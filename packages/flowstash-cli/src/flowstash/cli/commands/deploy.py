from typing import Optional
import asyncio
import typer
import click
from typer.core import TyperGroup
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
import questionary
from ..core.api_client import APIClient
from ..core.config import load_project_config, resolve_credentials
from .build import run_build_flow
import flowstash.runtime


class DefaultCommandGroup(TyperGroup):
    """Typer group that falls back to a default subcommand.

    Keeps ``flowstash deploy``, ``flowstash deploy <env>`` and
    ``flowstash deploy --artifact X`` working (they route to ``run``) while
    still allowing real subcommands such as ``configure``.
    """

    DEFAULT_CMD = "run"

    def parse_args(self, ctx, args):
        if not args:
            args = [self.DEFAULT_CMD]
        elif args[0] not in self.commands and args[0] != "--help":
            args = [self.DEFAULT_CMD, *args]
        return super().parse_args(ctx, args)


app = typer.Typer(cls=DefaultCommandGroup)
console = Console()

# Status labels shown to the user while polling
_STATUS_LABELS = {
    "QUEUED": "Queued, waiting for deployment to start...",
    "VALIDATING": "Validating container images...",
    "DEPLOYING": "Deploying services...",
    "HEALTH_CHECK": "Health-checking API and Worker...",
    "SYNCING_SCHEDULES": "Waiting for deployment verification...",
    "PROFILE_UPDATE": "Applying profile update...",
    "DEPLOYED": "Deployed successfully ✓",
    "FAILED": "Deployment failed.",
}

_TERMINAL_STATUSES = {"DEPLOYED", "FAILED"}


async def _poll_deploy_status(api: APIClient, deploy_id: str, progress, task):
    """Poll deploy status until terminal. Returns the final status payload.

    On FAILED, prints the backend error and raises typer.Exit(1).
    """
    last_status = None
    while True:
        status_data = await api.get(f"/v1/deploy/{deploy_id}/status")
        current_status = status_data["status"]

        if current_status != last_status:
            label = _STATUS_LABELS.get(current_status, f"Status: {current_status}")
            progress.update(task, description=label)
            last_status = current_status

        if current_status == "DEPLOYED":
            return status_data
        elif current_status == "FAILED":
            error = status_data.get("error_message", "Unknown error")
            console.print(f"[red]Deployment failed: {error}[/red]")
            raise typer.Exit(code=1)

        await asyncio.sleep(5)


async def run_deploy_flow(
    env: str, artifact_id: Optional[str] = None, user: Optional[str] = None
):
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
        status_data = await _poll_deploy_status(api, deploy_id, progress, task)

    return status_data


def _resolve_project_and_token(user: Optional[str]):
    """Load .flowstash project_id and resolve an access token, or exit."""
    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_id = project_config.project_id
    if not project_id:
        console.print(
            "[red]project_id not found in .flowstash. Use 'flowstash init' or link to a project.[/red]"
        )
        raise typer.Exit(code=1)

    token = resolve_credentials(user=user)
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    return project_config, project_id, token


def _spec_summary(spec: dict) -> str:
    """Render a compact one-line summary of a deployment-profile spec."""
    if not isinstance(spec, dict):
        return ""
    parts = []
    worker = spec.get("worker_service") or {}
    if worker:
        parts.append(
            f"worker cpu={worker.get('cpu')} mem={worker.get('memory')} "
            f"max={worker.get('max_instances')}"
        )
    api_svc = spec.get("api_service") or {}
    if api_svc:
        parts.append(f"api cpu={api_svc.get('cpu')} mem={api_svc.get('memory')}")
    return " / ".join(parts)


@app.command("run")
def deploy_run(
    ctx: typer.Context,
    env: str = typer.Argument("prod", help="Environment to deploy to (default: prod)"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", "-a", help="Artifact ID to deploy"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """
    [bold cyan]Deploy[/bold cyan] your project to the flowstash Managed Platform.

    Defaults to the 'prod' environment. If 'prod' is missing, it will prompt you to set it up.

    [yellow]Note:[/yellow] To deploy to a specific environment, use: [bold]flowstash deploy <env_name>[/bold]
    """
    from .project import add_environment, _link_project

    if ctx.get_parameter_source("env") == click.core.ParameterSource.DEFAULT:
        console.print(
            f"[dim]Env argument not specified... using [bold]{env}[/bold] as default[/dim]"
        )

    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    # Find the requested environment, offering to scaffold 'prod' if missing
    env_mode = next((e for e in project_config.environments if e.name == env), None)
    if not env_mode:
        if env == "prod":
            if yes or Confirm.ask(
                f"Environment '{env}' not found. Would you like to set it up now?"
            ):
                add_environment(project_config, env_name=env)
                project_config = load_project_config()
                env_mode = next(
                    (e for e in project_config.environments if e.name == env), None
                )
                if not env_mode:
                    console.print(
                        f"[red]Environment '{env}' was not created. Aborting.[/red]"
                    )
                    raise typer.Exit(code=1)
            else:
                console.print(
                    "[red]Aborting. Use 'flowstash env add' to create environments manually.[/red]"
                )
                raise typer.Exit(code=1)
        else:
            console.print(f"[red]Environment '{env}' not found.[/red]")
            console.print(
                f"[yellow]Available environments: {', '.join(e.name for e in project_config.environments)}[/yellow]"
            )
            console.print(
                "To deploy to a specific environment, use: [bold]flowstash deploy <env_name>[/bold]"
            )
            raise typer.Exit(code=1)

    # Ask for confirmation unless non-interactive is provided
    if not yes:
        if not Confirm.ask(f"Are you sure you want to deploy to '{env}'?"):
            console.print("Deployment cancelled.")
            raise typer.Exit(code=0)

    # Check if env is managed
    if not env_mode.managed:
        console.print(
            f"[red]Environment '{env}' is not managed. Deployment is only supported for managed environments.[/red]"
        )
        console.print(
            "[yellow]Update your .flowstash environments if this is incorrect.[/yellow]"
        )
        raise typer.Exit(code=1)

    project_id = project_config.project_id
    if not project_id:
        if not yes and Confirm.ask(
            "Project ID not found. Would you like to link to a managed project now?"
        ):
            _link_project(project_config, user=user)
            project_id = project_config.project_id

        if not project_id:
            console.print(
                "[red]project_id not found in .flowstash. Use 'flowstash init' or link to a project.[/red]"
            )
            raise typer.Exit(code=1)

    result = asyncio.run(run_deploy_flow(env, artifact, user=user))

    api_url = result.get("api_url", "")
    console.print("[green]✓ Deployment complete![/green]")
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


@app.command("configure")
def deploy_configure(
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Environment to configure"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", "-p", help="Deployment profile name"
    ),
    always_on: Optional[bool] = typer.Option(
        None,
        "--always-on/--no-always-on",
        help="Keep one worker warm (min_worker_instances 1) vs scale to zero (0)",
    ),
    apply: Optional[bool] = typer.Option(
        None,
        "--apply/--no-apply",
        help="Apply to running services immediately (skips the prompt)",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """
    [bold cyan]Configure[/bold cyan] the deployment profile and scaling for an environment.

    Interactively pick the environment, deployment profile, and whether the worker
    stays always-on. Provide [bold]--env[/bold], [bold]--profile[/bold] and
    [bold]--always-on/--no-always-on[/bold] to skip the prompts.
    """
    project_config, project_id, token = _resolve_project_and_token(user)
    api = APIClient(token=token)

    # 1. Environment selection (restricted to managed environments)
    managed_envs = [e for e in project_config.environments if e.managed]
    if env is None:
        if not managed_envs:
            console.print(
                "[red]No managed environments found in .flowstash. "
                "Add one with 'flowstash env add'.[/red]"
            )
            raise typer.Exit(code=1)
        env = questionary.select(
            "Environment to configure:",
            choices=[e.name for e in managed_envs],
        ).ask()
        if not env:
            console.print("Cancelled.")
            raise typer.Exit(code=0)
    else:
        env_mode = next((e for e in project_config.environments if e.name == env), None)
        if env_mode is not None and not env_mode.managed:
            console.print(
                f"[red]Environment '{env}' is not managed. Deployment configuration "
                "is only supported for managed environments.[/red]"
            )
            raise typer.Exit(code=1)

    # 2. Load available profiles + current config from the API
    async def _load():
        profiles = await api.get(
            "/v1/deployment-profiles", params={"project_id": project_id}
        )
        try:
            current = await api.get(
                f"/v1/environments/{project_id}/{env}/deployment-config"
            )
        except Exception:
            # First-time configuration: no stored config yet.
            current = {}
        return profiles, current

    try:
        profiles, current = asyncio.run(_load())
    except Exception as e:
        console.print(f"[red]Failed to load deployment profiles: {e}[/red]")
        raise typer.Exit(code=1)

    profile_names = [p.get("name") for p in (profiles or []) if p.get("name")]
    current_profile = (current or {}).get("deployment_profile")
    current_min = (current or {}).get("min_worker_instances") or 0

    # 3. Profile selection
    if profile is None:
        if not profile_names:
            console.print("[red]No deployment profiles available.[/red]")
            raise typer.Exit(code=1)
        spec_by_name = {p.get("name"): p.get("spec", {}) for p in profiles}
        choices = []
        for name in profile_names:
            summary = _spec_summary(spec_by_name.get(name, {}))
            label = f"{name}  —  {summary}" if summary else name
            choices.append(questionary.Choice(label, value=name))
        profile = questionary.select(
            "Deployment profile:",
            choices=choices,
            default=current_profile if current_profile in profile_names else None,
        ).ask()
        if not profile:
            console.print("Cancelled.")
            raise typer.Exit(code=0)
    elif profile_names and profile not in profile_names:
        console.print(
            f"[yellow]Warning: profile '{profile}' is not in the known list "
            f"({', '.join(profile_names)}). Sending anyway.[/yellow]"
        )

    # 4. Always-on flag -> min_worker_instances (boolean: 1 / 0)
    if always_on is None:
        always_on = Confirm.ask(
            "Keep a worker always on (no cold starts)?", default=current_min > 0
        )
    min_worker_instances = 1 if always_on else 0

    # 5. Summary + confirmation
    console.print()
    console.print(f"  Environment : [bold]{env}[/bold]")
    console.print(f"  Profile     : [bold]{profile}[/bold]")
    console.print(
        f"  Always on   : [bold]{'yes' if always_on else 'no'}[/bold] "
        f"(min_worker_instances={min_worker_instances})"
    )
    if not yes and not Confirm.ask("Save this deployment configuration?", default=True):
        console.print("Cancelled.")
        raise typer.Exit(code=0)

    # 6. Persist server-side
    async def _save():
        return await api.request(
            "PUT",
            f"/v1/environments/{project_id}/{env}/deployment-config",
            json={
                "deployment_profile": profile,
                "min_worker_instances": min_worker_instances,
            },
        )

    try:
        asyncio.run(_save())
    except Exception as e:
        console.print(f"[red]Failed to save deployment configuration: {e}[/red]")
        raise typer.Exit(code=1)

    console.print(f"[green]✓ Deployment configuration saved for '{env}'.[/green]")

    # 7. Offer to apply to running services
    if apply is None:
        apply = Confirm.ask("Apply to running services now?", default=False)
    if not apply:
        console.print(
            "[dim]Changes will take effect on the next 'flowstash deploy'.[/dim]"
        )
        return

    try:
        asyncio.run(_apply_profile(api, project_id, env))
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Failed to apply profile: {e}[/red]")
        raise typer.Exit(code=1)


async def _apply_profile(api: APIClient, project_id: str, env: str):
    """POST apply-profile and poll the resulting deploy, if any."""
    res = await api.request(
        "POST", f"/v1/environments/{project_id}/{env}/apply-profile"
    )

    if res.get("no_change"):
        console.print(
            f"[green]✓ {res.get('message', 'Profile configuration already applied.')}[/green]"
        )
        return

    deploy_id = res.get("deploy_id")
    if not deploy_id:
        console.print(
            f"[green]✓ {res.get('message', 'Profile update queued.')}[/green]"
        )
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task(
            description="Applying profile update...", total=None
        )
        result = await _poll_deploy_status(api, deploy_id, progress, task)

    console.print("[green]✓ Profile update applied.[/green]")
    console.print(f"  Deployment ID : [bold]{result.get('deploy_id', deploy_id)}[/bold]")

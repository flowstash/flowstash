import typer
import click
from typing import Optional
from .commands import auth as auth_cmds
from .commands import project as project_cmds
from .commands import build as build_cmds
from .commands import deploy as deploy_cmds
from .commands import run as run_cmds
from .commands import webhook as webhook_cmds
from .commands import apikey as apikey_cmds
from .commands import client as client_cmds

from rich.console import Console
import importlib.metadata

try:
    __version__ = importlib.metadata.version("flowstash-cli")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

console = Console()

app = typer.Typer(
    name="flowstash",
    help=f"""
# 🚀 flowstash CLI (v{__version__})
Manage your flowstash projects and deployments with ease.

## 🛠 Usage
Run commands below to initialize, build, and deploy your integrations.
""",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

env_app = typer.Typer(
    name="env",
    help="Manage environments",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
app.add_typer(env_app, name="env")

project_app = typer.Typer(
    name="project",
    help="Manage the current project (API keys, settings)",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
app.add_typer(project_app, name="project")

app.add_typer(
    apikey_cmds.app,
    name="api-keys",
    help="Manage observability API keys",
    no_args_is_help=True,
)

app.add_typer(
    webhook_cmds.app,
    name="webhook",
    help="Start a temporary public webhook listener to capture sample payloads",
    no_args_is_help=True,
)

app.add_typer(
    client_cmds.app,
    name="client",
    help="Inspect and interact with configured HTTP clients",
    no_args_is_help=True,
)


@app.command()
def help(ctx: typer.Context):
    """
    Show help information for all commands.
    """
    (
        console.print(ctx.parent.get_help())
        if ctx.parent
        else console.print(ctx.get_help())
    )


# Shortcuts / Top-level commands
@app.command()
def login(
    username: Optional[str] = typer.Option(
        None, "--username", "-u", help="Username for manual login"
    ),
    password: Optional[str] = typer.Option(
        None, "--password", "-p", help="Password for manual login"
    ),
):
    """Log in to the flowstash Managed Platform."""
    auth_cmds.login(username=username, password=password)


@app.command()
def init(
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Project name"),
    fix: bool = typer.Option(False, "--fix", help="Repair missing components"),
    force: bool = typer.Option(False, "--force", help="Force overwrite existing files"),
):
    """
    [bold green]Initialize[/bold green] or [bold yellow]repair[/bold yellow] a flowstash project.

    This command sets up the local structure, creates the `.flowstash` config,
    and lets you configure your first environments.
    """
    project_cmds.init(name=name, fix=fix, force=force)


@app.command()
def check():
    """Inspect the current project structure."""
    project_cmds.check()


@app.command()
def link():
    """
    [bold cyan]Link[/bold cyan] your current project to a flowstash Managed Project.
    """
    from .core.config import load_project_config
    from .commands.project import _link_project

    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    _link_project(project_config)


@env_app.command("list")
def env_list():
    """List all configured environments."""
    project_cmds.env_list()


@env_app.command("add")
def env_add(
    env_name: Optional[str] = typer.Argument(None, help="Environment name to add"),
    force: bool = typer.Option(False, "--force", help="Force overwrite existing files"),
):
    """
    [bold blue]Add a new environment[/bold blue] to your project.

    Choose between Local, Dev, Prod, or Custom. It will automatically filter
    out existing environments and scaffold necessary files.
    """
    project_cmds.env_add(env_name=env_name, force=force)


@env_app.command("del")
def env_del(env_name: str = typer.Argument(..., help="Environment name to delete")):
    """
    [bold red]Delete an environment[/bold red] from your project.

    Requires confirmation before removal.
    """
    project_cmds.env_delete(env_name=env_name)


@env_app.command("init-vscode")
def env_init_vscode(
    env_name: str = typer.Argument(
        ..., help="Environment name to initialize VS Code for"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force overwrite existing launch.json if it's malformed or already has the configuration",
    ),
):
    """
    [bold blue]Initialize VS Code launch configurations[/bold blue] for an environment.

    This command will add 'Launch <env> API' and 'Launch <env> Worker' to your .vscode/launch.json,
    allowing you to easily debug your environment's API and Worker using VS Code.
    """
    project_cmds.init_vscode_launch(env_name=env_name, force=force)


@app.command()
def run(
    ctx: typer.Context,
    env: str = typer.Argument("dev", help="Environment to run (default: local)"),
    build: bool = typer.Option(
        True, "--build/--no-build", help="Build images before starting"
    ),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run in background"),
    logs: bool = typer.Option(
        False, "--logs", help="Follow logs (useful with --detach)"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
):
    """
    [bold magenta]Run[/bold magenta] the project locally using Docker Compose.

    Defaults to the 'local' environment.

    [yellow]Note:[/yellow] To run a specific environment, use: [bold]flowstash run <env_name>[/bold]
    """
    from .core.config import load_project_config
    from rich.prompt import Confirm

    if ctx.get_parameter_source("env") == click.core.ParameterSource.DEFAULT:
        console.print(
            f"[dim]Env argument not specified... using [bold]{env}[/bold] as default[/dim]"
        )

    project_config = load_project_config()
    if project_config:
        env_mode = next((e for e in project_config.environments if e.name == env), None)
        if not env_mode:
            if env == "dev":
                if yes or Confirm.ask(
                    f"Environment '{env}' not found. Would you like to set it up now?"
                ):
                    project_cmds.add_environment(project_config, env_name=env)
                else:
                    console.print(
                        f"[red]Aborting. Run 'flowstash env add' to create it manually.[/red]"
                    )
                    raise typer.Exit(code=1)
            else:
                console.print(f"[red]Environment '{env}' not found.[/red]")
                raise typer.Exit(code=1)

    run_cmds.run(env=env, build=build, detach=detach, logs=logs)


@app.command()
def build(
    ctx: typer.Context,
    env: str = typer.Argument("dev", help="Environment to build (default: local)"),
    tag: str = typer.Option("latest", "--tag", "-t", help="Tag for the image"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account to use (default: project-linked or current)"
    ),
):
    """
    [bold yellow]Build[/bold yellow] project artifacts/images.

    Defaults to the 'local' environment.

    [yellow]Note:[/yellow] To build for a specific environment, use: [bold]flowstash build <env_name>[/bold]
    """
    if ctx.get_parameter_source("env") == click.core.ParameterSource.DEFAULT:
        console.print(
            f"[dim]Env argument not specified... using [bold]{env}[/bold] as default[/dim]"
        )

    from .core.config import load_project_config
    from rich.prompt import Confirm

    project_config = load_project_config()
    if project_config:
        env_mode = next((e for e in project_config.environments if e.name == env), None)
        if not env_mode:
            if env == "dev":
                if yes or Confirm.ask(
                    f"Environment '{env}' not found. Would you like to set it up now?"
                ):
                    project_cmds.add_environment(project_config, env_name=env)
                else:
                    console.print(
                        f"[red]Aborting. Run 'flowstash env add' to create it manually.[/red]"
                    )
                    raise typer.Exit(code=1)
            else:
                console.print(f"[red]Environment '{env}' not found.[/red]")
                raise typer.Exit(code=1)

    build_cmds.build(env=env, tag=tag, user=user)


@app.command()
def deploy(
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
    from .core.config import load_project_config
    from .commands.project import add_environment
    from rich.prompt import Confirm

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

    # Find the requested environment
    env_mode = next((e for e in project_config.environments if e.name == env), None)

    if not env_mode:
        if env == "prod":
            if yes or Confirm.ask(
                f"Environment '{env}' not found. Would you like to set it up now?"
            ):
                # We need to pass the actual project_config object to add_environment
                # But project_cmds is a module, we should be careful about cyclic imports or just use it.
                project_cmds.add_environment(project_config, env_name=env)
                # Reload or refresh check
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
                    f"[red]Aborting. Use 'flowstash env add' to create environments manually.[/red]"
                )
                raise typer.Exit(code=1)
        else:
            console.print(f"[red]Environment '{env}' not found.[/red]")
            console.print(
                f"[yellow]Available environments: {', '.join(e.name for e in project_config.environments)}[/yellow]"
            )
            console.print(
                f"To deploy to a specific environment, use: [bold]flowstash deploy <env_name>[/bold]"
            )
            raise typer.Exit(code=1)

    deploy_cmds.deploy(env=env, artifact=artifact, yes=yes, user=user)


@app.command()
def whoami(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Show status for a specific account"
    ),
):
    """Show current login status."""
    auth_cmds.whoami(user=user)


@app.command("logged-in")
def logged_in(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Show status for a specific account"
    ),
):
    """Show who is currently logged in."""
    auth_cmds.whoami(user=user)


@app.command()
def logout(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Account email to log out (default: current user)"
    ),
):
    """Log out from the platform."""
    auth_cmds.logout(user=user)


@app.command()
def accounts():
    """List all logged-in accounts."""
    auth_cmds.accounts()


@app.command()
def version():
    """Show the flowstash CLI version."""
    console.print(f"flowstash CLI version: [bold green]{__version__}[/bold green]")


if __name__ == "__main__":
    app()

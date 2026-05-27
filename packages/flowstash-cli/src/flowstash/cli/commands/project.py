from typing import Optional, List, Dict
import typer
from pathlib import Path
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from datetime import datetime
import asyncio
import questionary

from ..core.api_client import APIClient
from ..core.config import (
    get_access_token,
    resolve_credentials,
    load_project_config,
    save_project_config,
    ProjectConfig,
    EnvironmentMode,
)

app = typer.Typer()
console = Console()

import shutil


def find_project_root(start_path: Path = Path.cwd()) -> Optional[Path]:
    """Find the nearest project root marked by .flowstash or pyproject.toml."""
    for parent in [start_path] + list(start_path.parents):
        if (parent / ".flowstash").exists() or (parent / "pyproject.toml").exists():
            return parent
    return None


def has_env_descendant(path: Path) -> bool:
    """Check if a directory contains any [env] folder in its descendants."""
    if not path.is_dir():
        return False
    if path.name == "[env]":
        return True
    for child in path.iterdir():
        if child.name == "[env]":
            return True
        if child.is_dir() and has_env_descendant(child):
            return True
    return False


def process_node(
    src: Path,
    dest_parent: Path,
    env_mode: Optional[EnvironmentMode],
    is_fix: bool,
    is_check: bool,
    force: bool,
    project_name: str,
    root_path: Path,
    errors: list,
    target_env: Optional[str] = None,
):
    name = src.name

    # If target_env is set and we are NOT in an environment-specific context yet
    if target_env and not env_mode:
        # If this IS the [env] folder, we only care about target_env
        if name == "[env]":
            pass  # Continue to [env] handling below
        elif src.is_dir():
            # If this is a directory, only proceed if it contains an [env] somewhere
            if not has_env_descendant(src):
                return
        else:
            # If this is a file at the root or outside [env], skip it for env-add
            return

    # 1. Condition check (...)
    if name.startswith("(") and name.endswith(")"):
        condition = name[1:-1]
        if "-" in condition:
            q, a = condition.split("-", 1)
            if not env_mode:
                return
            if env_mode.options.get(q) != a:
                return

            # Condition MET. Process its children and place them in dest_parent
            if src.is_dir():
                for child in src.iterdir():
                    process_node(
                        child,
                        dest_parent,
                        env_mode,
                        is_fix,
                        is_check,
                        force,
                        project_name,
                        root_path,
                        errors,
                        target_env=target_env,
                    )
            return

    # 2. Iterate environments [env]
    if name == "[env]":
        if src.is_dir():
            project_config = load_project_config()
            envs = project_config.environments if project_config else []

            # If target_env is specified, only process that one
            if target_env:
                envs = [e for e in envs if e.name == target_env]

            for em in envs:
                new_dest = dest_parent / em.name
                for child in src.iterdir():
                    process_node(
                        child,
                        new_dest,
                        em,
                        is_fix,
                        is_check,
                        force,
                        project_name,
                        root_path,
                        errors,
                        target_env=target_env,
                    )
        return

    # 3. Handle `_` prefix
    is_underscored = False
    target_name = name
    if target_name.startswith("_") and not target_name.startswith("__"):
        is_underscored = True
        target_name = target_name[1:]

    target_path = dest_parent / target_name

    # 4. Handle Directory vs File
    if src.is_dir():
        if not is_check:
            should_create = True
            if not target_path.exists():
                if is_underscored and is_fix and not force:
                    should_create = False

                if should_create:
                    target_path.mkdir(parents=True, exist_ok=True)

        else:
            if is_underscored and not target_path.exists():
                errors.append(
                    f"Missing mandatory folder: [bold]{target_path.relative_to(root_path)}[/bold]"
                )

        # Recurse children
        for child in src.iterdir():
            process_node(
                child,
                target_path,
                env_mode,
                is_fix,
                is_check,
                force,
                project_name,
                root_path,
                errors,
                target_env=target_env,
            )

    else:
        # File
        if not is_check:
            should_create = True
            if target_path.exists() and not force:
                should_create = False

            if not target_path.exists():
                if is_underscored and is_fix and not force:
                    should_create = False

            if should_create:
                try:
                    content = src.read_text()
                    content = content.replace("{project_name}", project_name)
                    project_name_slug = (
                        project_name.lower().replace("_", "-").strip("-")
                    )
                    if not project_name_slug:
                        project_name_slug = "flowstash-project"
                    content = content.replace("{project_name_slug}", project_name_slug)
                    if env_mode:
                        content = content.replace("{env}", env_mode.name)
                        content = content.replace("[env]", env_mode.name)

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_text(content)
                    status = "Updated" if target_path.exists() and force else "Created"

                    # Compute relative path or just use absolute if not under root
                    try:
                        rel_path = target_path.relative_to(root_path)
                    except ValueError:
                        rel_path = target_path
                    console.print(f"{status}: [bold]{rel_path}[/bold]")
                except Exception as e:
                    console.print(f"[red]Failed to process {src.name}: {e}[/red]")
        else:
            if is_underscored and not target_path.exists():
                try:
                    rel_path = target_path.relative_to(root_path)
                except ValueError:
                    rel_path = target_path
                errors.append(f"Missing mandatory file: [bold]{rel_path}[/bold]")


@app.command()
def check():
    """Inspect the current project against the spec and produce a report."""
    root = find_project_root()
    if not root:
        console.print(
            "[red]Not in a flowstash project (no .flowstash or pyproject.toml found).[/red]"
        )
        raise typer.Exit(code=1)

    console.print(f"Project root: [bold]{root}[/bold]")

    project_config = load_project_config()
    errors = []

    if (root / ".flowstash").exists():
        if project_config is None:
            errors.append(
                "[bold].flowstash[/bold] exists but is invalid YAML or has a wrong schema."
            )
        else:
            if not project_config.project_name:
                errors.append(
                    "[bold].flowstash[/bold] is missing [bold]project_name[/bold]."
                )

    project_name = project_config.project_name if project_config else root.name

    templates_dir = Path(__file__).parent.parent / "templates"
    for child in templates_dir.iterdir():
        process_node(
            child,
            root,
            None,
            is_fix=False,
            is_check=True,
            force=False,
            project_name=project_name,
            root_path=root,
            errors=errors,
        )

    if not errors:
        console.print("[green]Project structure is valid![/green]")
    else:
        console.print("[red]Structure issues found:[/red]")
        for err in errors:
            console.print(f"  - {err}")
        console.print(
            "\n[yellow]Run 'flowstash init --fix' to attempt repair.[/yellow]"
        )


def init_env(env_name: str) -> EnvironmentMode:
    answers = {}

    # Backend
    backend_choices = [
        questionary.Choice(
            "Asyncio - only for local development... not recomended for production",
            "asyncio",
        ),
        questionary.Choice(
            "Dramatiq task queue - ideal for dev envirment / self hosting", "dramatiq"
        ),
    ]
    if env_name != "dev":
        backend_choices.append(
            questionary.Choice(
                "Cloud managed backend, scales from 0->inf+ ... ideal for hasle free",
                "managed",
            )
        )

    backend = questionary.select(
        "Backend type:\nPick one of these backend types:", choices=backend_choices
    ).ask()
    if backend:
        answers["backend"] = backend

    # Observability
    obs_choices = [
        questionary.Choice("None or custom observability", "unset"),
        questionary.Choice("Log events to log file", "logfile"),
        # questionary.Choice("OpenTelemetry", "otel"),
        questionary.Choice("Use flowstash managed monitoring", "managed"),
    ]
    obs = questionary.select("Observability:", choices=obs_choices).ask()
    if obs:
        answers["observability"] = obs

    is_managed = (answers.get("backend") == "managed") or (
        answers.get("observability") == "managed"
    )
    return EnvironmentMode(name=env_name, managed=is_managed, options=answers)


def init_vscode_launch(env_name: str, force: bool = False):
    """Initialize VS Code launch settings for a specific environment."""
    root = find_project_root()
    if not root:
        console.print(
            "[red]Not in a flowstash project. Run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_config = load_project_config()
    if not project_config:
        console.print("[red]Invalid project configuration.[/red]")
        raise typer.Exit(code=1)

    # Find the environment
    env_to_init = next(
        (e for e in project_config.environments if e.name == env_name), None
    )
    if not env_to_init:
        console.print(f"[red]Environment '{env_name}' not found.[/red]")
        raise typer.Exit(code=1)

    if env_to_init.managed:
        console.print(
            f"[red]Cannot initialize VS Code launch for managed environment '{env_name}'.[/red]"
        )
        raise typer.Exit(code=1)

    vscode_dir = root / ".vscode"
    vscode_dir.mkdir(parents=True, exist_ok=True)
    launch_json_path = vscode_dir / "launch.json"

    import json

    launch_data = {"version": "0.2.0", "configurations": []}
    if launch_json_path.exists():
        try:
            with open(launch_json_path, "r") as f:
                launch_data = json.load(f)
        except json.JSONDecodeError:
            # Maybe there are comments or it's malformed. We'll ask to overwrite.
            if not force:
                console.print(
                    f"[yellow]Warning: {launch_json_path} contains invalid JSON or comments. Cannot parse it automatically. Use --force to overwrite it entirely.[/yellow]"
                )
                raise typer.Exit(code=1)
            launch_data = {"version": "0.2.0", "configurations": []}

    api_config_name = f"Launch {env_name} API"
    worker_config_name = f"Launch {env_name} Worker"

    existing_configs = {
        cfg.get("name") for cfg in launch_data.get("configurations", [])
    }

    added = False

    if api_config_name not in existing_configs or force:
        # Remove old if force
        launch_data["configurations"] = [
            c
            for c in launch_data.setdefault("configurations", [])
            if c.get("name") != api_config_name
        ]

        launch_data["configurations"].append(
            {
                "name": api_config_name,
                "type": "debugpy",
                "request": "launch",
                "program": "${workspaceFolder}/api_main.py",
                "console": "integratedTerminal",
                "env": {
                    "ENVIRONMENT": env_name,
                    "PYTHONPATH": "${workspaceFolder}:${workspaceFolder}/src",
                },
            }
        )
        added = True

    if worker_config_name not in existing_configs or force:
        # Remove old if force
        launch_data["configurations"] = [
            c
            for c in launch_data.setdefault("configurations", [])
            if c.get("name") != worker_config_name
        ]

        launch_data["configurations"].append(
            {
                "name": worker_config_name,
                "type": "debugpy",
                "request": "launch",
                "program": "${workspaceFolder}/worker_main.py",
                "console": "integratedTerminal",
                "env": {
                    "ENVIRONMENT": env_name,
                    "PYTHONPATH": "${workspaceFolder}:${workspaceFolder}/src",
                },
            }
        )
        added = True

    if added:
        with open(launch_json_path, "w") as f:
            json.dump(launch_data, f, indent=4)
        console.print(
            f"[green]Added VS Code launch configurations for '{env_name}' to {launch_json_path}[/green]"
        )
    else:
        console.print(f"VS Code launch configurations already exist for '{env_name}'.")


def add_environment(project_config: ProjectConfig, env_name: Optional[str] = None):
    if not env_name:
        existing_envs = {e.name for e in project_config.environments}
        env_choices = [
            questionary.Choice("Development (dev)", "dev"),
            questionary.Choice("Production (prod)", "prod"),
            questionary.Choice("Custom", "custom"),
        ]
        # Filter out existing (except custom)
        env_choices = [
            c
            for c in env_choices
            if c.value == "custom" or c.value not in existing_envs
        ]

        env_sel = questionary.select("Environment name:", choices=env_choices).ask()

        if not env_sel:
            return

        if env_sel == "custom":
            env_name = Prompt.ask("Enter custom environment name")
        else:
            env_name = env_sel

    if not env_name or any(e.name == env_name for e in project_config.environments):
        console.print("[red]Environment already exists or invalid name![/red]")
        return None

    mode = init_env(env_name)
    project_config.environments.append(mode)
    save_project_config(project_config)
    return mode


@app.command("list")
def env_list():
    """List all environments configured in the current project."""
    project_config = load_project_config()
    if not project_config:
        console.print("[red]Not in a flowstash project or invalid configuration.[/red]")
        raise typer.Exit(code=1)

    envs = project_config.environments
    if not envs:
        console.print("[yellow]No environments configured.[/yellow]")
        return

    table = Table(title="Environments", show_header=True, header_style="bold cyan")
    table.add_column("Name", style="bold")
    table.add_column("Managed", justify="center")
    table.add_column("Options")

    for env in envs:
        managed_str = "[green]yes[/green]" if env.managed else "[dim]no[/dim]"
        options_str = ", ".join(f"{k}={v}" for k, v in (env.options or {}).items())
        table.add_row(env.name, managed_str, options_str or "[dim]-[/dim]")

    console.print(table)


@app.command("add")
def env_add(
    env_name: Optional[str] = typer.Argument(None, help="Environment name to add"),
    force: bool = typer.Option(False, "--force", help="Force overwrite existing files"),
):
    """Add a new environment to the project."""
    root = find_project_root()
    if not root:
        console.print(
            "[red]Not in a flowstash project. Run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_config = load_project_config()
    if not project_config:
        console.print("[red]Invalid project configuration.[/red]")
        raise typer.Exit(code=1)

    added_mode = add_environment(project_config, env_name=env_name)
    if not added_mode:
        return

    # Reload config to get the newly added environment
    project_config = load_project_config()

    # We now know the actual environment name picked
    env_name = added_mode.name

    # Check if we should ask for linkage
    is_managed = any(e.managed for e in project_config.environments)
    if not project_config.project_id and is_managed:
        if (
            Prompt.ask("Link to managed project?", choices=["y", "n"], default="y")
            == "y"
        ):
            _link_project(project_config)

    # Process templates for the new environment
    templates_dir = Path(__file__).parent.parent / "templates"
    errors = []
    project_name = project_config.project_name or root.name

    for child in templates_dir.iterdir():
        process_node(
            child,
            root,
            None,
            is_fix=True,
            is_check=False,
            force=force,
            project_name=project_name,
            root_path=root,
            errors=errors,
            target_env=env_name,
        )

    console.print(f"[green]Environment configuration updated![/green]")

    if not added_mode.managed:
        if (
            Prompt.ask(
                "Would you like to initialize VS Code launch configurations for this environment?",
                choices=["y", "n"],
                default="y",
            )
            == "y"
        ):
            init_vscode_launch(env_name=env_name, force=force)


@app.command("delete")
def env_delete(
    env_name: str = typer.Argument(..., help="Name of the environment to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """
    [bold red]Delete an environment[/bold red] from your project.

    This will remove the environment from .flowstash and optionally delete its configuration folder.
    """
    root = find_project_root()
    if not root:
        console.print(
            "[red]Not in a flowstash project. Run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_config = load_project_config()
    if not project_config:
        console.print("[red]Invalid project configuration.[/red]")
        raise typer.Exit(code=1)

    # Find the environment
    env_to_del = next(
        (e for e in project_config.environments if e.name == env_name), None
    )
    if not env_to_del:
        console.print(f"[red]Environment '{env_name}' not found.[/red]")
        console.print(
            f"[yellow]Available environments: {', '.join(e.name for e in project_config.environments)}[/yellow]"
        )
        raise typer.Exit(code=1)

    from rich.prompt import Confirm

    if not yes and not Confirm.ask(
        f"Are you sure you want to delete environment [bold red]{env_name}[/bold red]?"
    ):
        console.print("Cancelled.")
        return

    # Remove from config
    project_config.environments = [
        e for e in project_config.environments if e.name != env_name
    ]
    save_project_config(project_config)

    # Optionally delete the folder
    env_folder = root / env_name
    if env_folder.exists() and env_folder.is_dir():
        if yes or Confirm.ask(
            f"Do you also want to delete the configuration folder [bold]{env_name}/[/bold]?"
        ):
            import shutil

            shutil.rmtree(env_folder)
            console.print(f"Deleted folder: [bold]{env_name}/[/bold]")

    console.print(f"[green]Environment '{env_name}' deleted successfully![/green]")


@app.command()
def init(
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Project name"),
    fix: bool = typer.Option(False, "--fix", help="Repair missing components"),
    force: bool = typer.Option(False, "--force", help="Force overwrite existing files"),
):
    """Initialize or repair a flowstash project."""
    root = Path.cwd()
    project_config = load_project_config()

    if project_config and not fix:
        if (
            not Prompt.ask(
                "Project already initialized. Re-initialize?",
                choices=["y", "n"],
                default="n",
            )
            == "y"
        ):
            return

    if not name:
        name = project_config.project_name if project_config else root.name
        name = Prompt.ask("Project name", default=name)

    if not project_config:
        project_config = ProjectConfig(project_name=name)
        save_project_config(project_config)
        console.print(f"[green]Created .flowstash with project_name: {name}[/green]")
    else:
        project_config.project_name = name
        save_project_config(project_config)

    # Environments Setup
    if not project_config.environments:
        # Ask to configure at least the first environment
        setup_first = Prompt.ask(
            "Setup first eniroment? y/n?", choices=["y", "n"], default="y"
        )
        if setup_first == "y":
            added_mode = add_environment(project_config)
            if added_mode and not added_mode.managed:
                if (
                    Prompt.ask(
                        f"Would you like to initialize VS Code launch configurations for {added_mode.name}?",
                        choices=["y", "n"],
                        default="y",
                    )
                    == "y"
                ):
                    init_vscode_launch(env_name=added_mode.name, force=force)
    else:
        if not fix:
            while (
                Prompt.ask(
                    "Add another enviroment? y/n?", choices=["y", "n"], default="n"
                )
                == "y"
            ):
                added_mode = add_environment(project_config)
                if added_mode and not added_mode.managed:
                    if (
                        Prompt.ask(
                            f"Would you like to initialize VS Code launch configurations for {added_mode.name}?",
                            choices=["y", "n"],
                            default="y",
                        )
                        == "y"
                    ):
                        init_vscode_launch(env_name=added_mode.name, force=force)

    templates_dir = Path(__file__).parent.parent / "templates"
    errors = []

    for child in templates_dir.iterdir():
        process_node(
            child,
            root,
            None,
            is_fix=fix,
            is_check=False,
            force=force,
            project_name=name,
            root_path=root,
            errors=errors,
        )

    # Prompt for project linkage if not linked AND handled by managed services
    is_managed = any(e.managed for e in project_config.environments)
    if not project_config.project_id and is_managed:
        if (
            Prompt.ask("Link to managed project?", choices=["y", "n"], default="y")
            == "y"
        ):
            _link_project(project_config)

    console.print("[green]Initialization/Repair complete![/green]")


def _link_project(config: ProjectConfig, user: Optional[str] = None):
    token = resolve_credentials(user=user)
    if not token:
        console.print(
            "[yellow]Not logged in. Use 'flowstash login' to link to a managed project.[/yellow]"
        )
        return

    api = APIClient(token=token)
    try:
        projects_data = asyncio.run(api.get("/v1/projects"))
        projects = projects_data.get("projects", [])
    except Exception as e:
        console.print(f"[red]Failed to fetch projects: {e}[/red]")
        return

    choices = ["Create new project"] + [
        f"{p['name']} ({p['project_id']})" for p in projects
    ]
    choice = questionary.select("Select a managed project", choices=choices).ask()

    if not choice:
        return

    if choice == "Create new project":
        name = Prompt.ask(
            "Enter name for new managed project", default=config.project_name
        )
        try:
            project = asyncio.run(api.post("/v1/projects", json={"name": name}))
            project_id = project["project_id"]
        except Exception as e:
            console.print(f"[red]Failed to create project: {e}[/red]")
            return
    else:
        project_id = choice.split("(")[-1].rstrip(")")

    # Fetch tenant
    try:
        user_info = asyncio.run(api.get("/v1/auth/me"))
        config.tenant_id = user_info["tenant_id"]
        config.user_email = user_info.get("email")
        config.linked_user = user_info.get("email")  # pin project to this account
    except Exception:
        config.tenant_id = "default"

    config.project_id = project_id
    save_project_config(config)
    console.print(f"[green]Linked to project_id: {project_id}[/green]")

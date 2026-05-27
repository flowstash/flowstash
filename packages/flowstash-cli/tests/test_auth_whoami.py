from typer.testing import CliRunner

from flowstash.cli.main import app


def test_whoami_shows_user_login_from_auth_me(monkeypatch):
    monkeypatch.setattr(
        "flowstash.cli.commands.auth.resolve_credentials", lambda user=None: "token"
    )

    class GlobalConfig:
        api_url = "https://api.flowstash.dev"
        accounts = []
        current_user = "user@example.com"

    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig()
    )
    monkeypatch.setattr("flowstash.cli.commands.auth.load_project_config", lambda: None)
    monkeypatch.setattr(
        "flowstash.cli.commands.auth._fetch_current_user",
        lambda token=None: {"email": "user@example.com", "tenant_id": "tenant-123"},
    )

    result = CliRunner().invoke(app, ["whoami"])

    assert result.exit_code == 0
    assert "Logged in as: user@example.com" in result.stdout
    assert "Tenant: tenant-123" in result.stdout


def test_whoami_falls_back_to_project_login_when_auth_me_fails(monkeypatch):
    monkeypatch.setattr(
        "flowstash.cli.commands.auth.resolve_credentials", lambda user=None: "token"
    )

    class GlobalConfig:
        api_url = "https://api.flowstash.dev"
        accounts = []
        current_user = None

    class ProjectConfig:
        user_email = "fallback@example.com"
        tenant_id = "tenant-fallback"
        project_id = "project-123"
        linked_user = None

    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig()
    )
    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_project_config", lambda: ProjectConfig()
    )
    monkeypatch.setattr(
        "flowstash.cli.commands.auth._fetch_current_user", lambda token=None: None
    )

    result = CliRunner().invoke(app, ["whoami"])

    assert result.exit_code == 0
    assert "Logged in as: fallback@example.com" in result.stdout
    assert "Tenant: tenant-fallback" in result.stdout
    assert "Current Project: project-123" in result.stdout


def test_whoami_with_explicit_user_flag(monkeypatch):
    monkeypatch.setattr(
        "flowstash.cli.commands.auth.resolve_credentials",
        lambda user=None: "token-alice" if user == "alice@example.com" else None,
    )

    class GlobalConfig:
        api_url = "https://api.flowstash.dev"
        accounts = []
        current_user = "bob@example.com"

    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig()
    )
    monkeypatch.setattr("flowstash.cli.commands.auth.load_project_config", lambda: None)
    monkeypatch.setattr(
        "flowstash.cli.commands.auth._fetch_current_user",
        lambda token=None: {"email": "alice@example.com", "tenant_id": "tenant-alice"},
    )

    result = CliRunner().invoke(app, ["whoami", "--user", "alice@example.com"])

    assert result.exit_code == 0
    assert "Logged in as: alice@example.com" in result.stdout


def test_accounts_lists_all_sessions(monkeypatch):
    class Account:
        def __init__(self, email, display_name, tenant_id):
            self.email = email
            self.display_name = display_name
            self.tenant_id = tenant_id

    class GlobalConfig:
        accounts = [
            Account("alice@example.com", "Alice", "tenant-a"),
            Account("bob@example.com", "Bob", "tenant-b"),
        ]
        current_user = "alice@example.com"

    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig()
    )

    result = CliRunner().invoke(app, ["accounts"])

    assert result.exit_code == 0
    assert "alice@example.com" in result.stdout
    assert "bob@example.com" in result.stdout


def test_accounts_empty(monkeypatch):
    class GlobalConfig:
        accounts = []
        current_user = None

    monkeypatch.setattr(
        "flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig()
    )

    result = CliRunner().invoke(app, ["accounts"])

    assert result.exit_code == 0
    assert "No accounts logged in" in result.stdout


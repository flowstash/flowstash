from typer.testing import CliRunner

from flowstash.cli.main import app


def test_whoami_shows_user_login_from_auth_me(monkeypatch):
    monkeypatch.setattr("flowstash.cli.commands.auth.get_access_token", lambda: "token")

    class GlobalConfig:
        api_url = "https://api.flowstash.dev"

    monkeypatch.setattr("flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig())
    monkeypatch.setattr("flowstash.cli.commands.auth.load_project_config", lambda: None)
    monkeypatch.setattr(
        "flowstash.cli.commands.auth._fetch_current_user",
        lambda: {"email": "user@example.com", "tenant_id": "tenant-123"},
    )

    result = CliRunner().invoke(app, ["whoami"])

    assert result.exit_code == 0
    assert "Logged in as: user@example.com" in result.stdout
    assert "Tenant: tenant-123" in result.stdout


def test_whoami_falls_back_to_project_login_when_auth_me_fails(monkeypatch):
    monkeypatch.setattr("flowstash.cli.commands.auth.get_access_token", lambda: "token")

    class GlobalConfig:
        api_url = "https://api.flowstash.dev"

    class ProjectConfig:
        user_email = "fallback@example.com"
        tenant_id = "tenant-fallback"
        project_id = "project-123"

    monkeypatch.setattr("flowstash.cli.commands.auth.load_global_config", lambda: GlobalConfig())
    monkeypatch.setattr("flowstash.cli.commands.auth.load_project_config", lambda: ProjectConfig())
    monkeypatch.setattr("flowstash.cli.commands.auth._fetch_current_user", lambda: None)

    result = CliRunner().invoke(app, ["whoami"])

    assert result.exit_code == 0
    assert "Logged in as: fallback@example.com" in result.stdout
    assert "Tenant: tenant-fallback" in result.stdout
    assert "Current Project: project-123" in result.stdout
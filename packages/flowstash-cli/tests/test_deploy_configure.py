from typer.testing import CliRunner

from flowstash.cli.main import app


class _Env:
    def __init__(self, name, managed=True):
        self.name = name
        self.managed = managed


class _ProjectConfig:
    project_id = "proj-123"
    environments = [_Env("prod", managed=True), _Env("dev", managed=False)]


_PROFILES = [
    {"name": "micro", "is_builtin": True, "spec": {}},
    {
        "name": "medium",
        "is_builtin": True,
        "spec": {
            "worker_service": {"cpu": "2", "memory": "2Gi", "max_instances": 10},
            "api_service": {"cpu": "1", "memory": "512Mi"},
        },
    },
]


class FakeAPI:
    """Records calls and returns canned responses keyed by (method, path)."""

    def __init__(self, token=None, responses=None):
        self.token = token
        self.calls = []
        # path -> response, or (method, path) -> response
        self.responses = responses or {}
        # status poll responses, consumed in order
        self.status_queue = []

    async def get(self, path, params=None):
        self.calls.append(("GET", path, {"params": params}))
        if path == "/v1/deployment-profiles":
            return _PROFILES
        if path.endswith("/deployment-config"):
            return self.responses.get(
                "deployment-config",
                {"deployment_profile": "micro", "min_worker_instances": 0},
            )
        if path.endswith("/status"):
            if self.status_queue:
                return self.status_queue.pop(0)
            return {"status": "DEPLOYED", "deploy_id": "dep-1"}
        return {}

    async def post(self, path, json=None, timeout=30.0):
        self.calls.append(("POST", path, {"json": json}))
        return self.responses.get(("POST", path), {})

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if path.endswith("/apply-profile"):
            return self.responses.get("apply-profile", {"no_change": True})
        return {}


def _patch(monkeypatch, fake):
    monkeypatch.setattr(
        "flowstash.cli.commands.deploy.load_project_config", lambda: _ProjectConfig()
    )
    monkeypatch.setattr(
        "flowstash.cli.commands.deploy.resolve_credentials", lambda user=None: "token"
    )
    monkeypatch.setattr(
        "flowstash.cli.commands.deploy.APIClient", lambda token=None: fake
    )


def test_configure_non_interactive_puts_deployment_config(monkeypatch):
    fake = FakeAPI()
    _patch(monkeypatch, fake)

    result = CliRunner().invoke(
        app,
        ["deploy", "configure", "-e", "prod", "-p", "medium", "--always-on", "-y", "--no-apply"],
    )

    assert result.exit_code == 0, result.stdout
    puts = [c for c in fake.calls if c[0] == "PUT"]
    assert len(puts) == 1
    method, path, kwargs = puts[0]
    assert path == "/v1/environments/proj-123/prod/deployment-config"
    assert kwargs["json"] == {
        "deployment_profile": "medium",
        "min_worker_instances": 1,
    }
    # --no-apply => apply-profile is never called
    assert not any(c[1].endswith("/apply-profile") for c in fake.calls)


def test_configure_no_always_on_sets_zero(monkeypatch):
    fake = FakeAPI()
    _patch(monkeypatch, fake)

    result = CliRunner().invoke(
        app,
        ["deploy", "configure", "-e", "prod", "-p", "micro", "--no-always-on", "-y", "--no-apply"],
    )

    assert result.exit_code == 0, result.stdout
    put = next(c for c in fake.calls if c[0] == "PUT")
    assert put[2]["json"]["min_worker_instances"] == 0


def test_configure_apply_no_change(monkeypatch):
    fake = FakeAPI(responses={"apply-profile": {"no_change": True, "message": "Profile configuration already applied"}})
    _patch(monkeypatch, fake)

    result = CliRunner().invoke(
        app,
        ["deploy", "configure", "-e", "prod", "-p", "medium", "--always-on", "-y", "--apply"],
    )

    assert result.exit_code == 0, result.stdout
    assert any(c[1].endswith("/apply-profile") for c in fake.calls)
    # no_change => no status polling
    assert not any(c[1].endswith("/status") for c in fake.calls)
    assert "already applied" in result.stdout.lower()


def test_configure_apply_polls_until_deployed(monkeypatch):
    fake = FakeAPI(
        responses={
            "apply-profile": {
                "no_change": False,
                "deploy_id": "dep-9",
                "status": "QUEUED",
            }
        }
    )
    fake.status_queue = [{"status": "DEPLOYED", "deploy_id": "dep-9"}]
    _patch(monkeypatch, fake)

    result = CliRunner().invoke(
        app,
        ["deploy", "configure", "-e", "prod", "-p", "medium", "--always-on", "-y", "--apply"],
    )

    assert result.exit_code == 0, result.stdout
    assert any(c[1] == "/v1/deploy/dep-9/status" for c in fake.calls)
    assert "Profile update applied" in result.stdout


def test_deploy_positional_still_routes_to_run(monkeypatch):
    """Backward compat: `flowstash deploy prod` still triggers a deploy."""
    called = {}

    async def fake_flow(env, artifact_id=None, user=None):
        called["env"] = env
        return {"deploy_id": "dep-1", "api_url": "https://x", "scheduled_tasks": None}

    monkeypatch.setattr(
        "flowstash.cli.commands.deploy.load_project_config", lambda: _ProjectConfig()
    )
    monkeypatch.setattr(
        "flowstash.cli.commands.deploy.run_deploy_flow", fake_flow
    )

    result = CliRunner().invoke(app, ["deploy", "prod", "-y"])

    assert result.exit_code == 0, result.stdout
    assert called.get("env") == "prod"
    assert "Deployment complete" in result.stdout


def test_deploy_group_help_lists_subcommands():
    result = CliRunner().invoke(app, ["deploy", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout
    assert "configure" in result.stdout

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import httpx
import pytest
from typer.testing import CliRunner

from flowstash.cli.main import app
from flowstash.cli.core import config
from flowstash.cli.core.diagnostics import DiagnosticError, time_window

runner = CliRunner()


@pytest.fixture
def api(monkeypatch, tmp_path, respx_mock):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FLOWSTASH_API_KEY", "secret-key-do-not-leak")
    monkeypatch.setenv("FLOWSTASH_API_URL", "https://test.flowstash.invalid")
    monkeypatch.delenv("FLOWSTASH_USER", raising=False)
    monkeypatch.setattr(config, "GLOBAL_CONFIG_FILE", tmp_path / "absent-global.yaml")
    (tmp_path / ".flowstash").write_text("project_id: p\ntenant_id: t\nenvironments:\n- name: prod\n  managed: true\n- name: dev\n  managed: false\n")
    state = {"requests": [], "identity": {"tenant_id": "t", "email": None}, "result": {"logs": [], "next_page_token": None}}

    def respond(request):
        state["requests"].append(request)
        assert request.headers["Authorization"] == "Bearer " + state.get("token", "secret-key-do-not-leak")
        path = unquote(request.url.path)
        if path == "/v1/auth/me":
            return httpx.Response(state.get("auth_status", 200), json=state["identity"])
        if path.startswith("/v1/projects/"):
            return httpx.Response(200, json={"project_id": path.split("/")[-1], "tenant_id": "t", "name": "Shop"})
        if path.endswith("/status"):
            return httpx.Response(200, json=state.get("build", {"project_id": "p", "build_id": "b"}))
        return httpx.Response(state.get("status", 200), json=state["result"])

    respx_mock.route().mock(side_effect=respond)
    return state


def invoke(args):
    return runner.invoke(app, args + ["--json"])


def test_context_subdirectory_and_no_side_effects(api, tmp_path, monkeypatch):
    child = tmp_path / "worktree" / "src"
    child.mkdir(parents=True)
    (child / "pyproject.toml").write_text("unrelated")
    before = (tmp_path / ".flowstash").read_bytes()
    monkeypatch.chdir(child)
    result = invoke(["context"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "project_root": str(tmp_path), "project_id": "p", "project_name": "Shop",
        "tenant_id": "t", "environment": "prod", "managed": True,
        "api_url": "https://test.flowstash.invalid", "auth_method": "api_key",
        "user_email": None, "project_source": "local_config",
    }
    assert result.stderr == ""
    assert "secret" not in result.output
    assert (tmp_path / ".flowstash").read_bytes() == before
    assert config.Path.cwd() == child


def test_nearest_nested_project_and_explicit_override(api, tmp_path, monkeypatch):
    child = tmp_path / "nested"
    child.mkdir()
    (child / ".flowstash").write_text("project_id: nested\n")
    monkeypatch.chdir(child)
    assert json.loads(invoke(["context"]).stdout)["project_id"] == "nested"
    result = json.loads(invoke(["context", "--project", "override"]).stdout)
    assert result["project_source"] == "explicit"
    assert result["project_id"] == "override"


@pytest.mark.parametrize("contents", ["[broken", "project_id: [wrong]", "just a string", "- a list"])
def test_malformed_nearest_config_is_error(api, tmp_path, monkeypatch, contents):
    child = tmp_path / "nested"
    child.mkdir()
    (child / ".flowstash").write_text(contents)
    monkeypatch.chdir(child)
    result = invoke(["context", "--project", "p"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert json.loads(result.stderr)["error"]["code"] == "INVALID_CONTEXT"
    assert not api["requests"]


def test_unlinked_and_unrelated_pyproject(api, tmp_path):
    (tmp_path / ".flowstash").unlink()
    (tmp_path / "pyproject.toml").write_text("[project]\nname='not-managed'")
    result = invoke(["context"])
    assert result.exit_code == 2
    result = invoke(["context", "--project", "p"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["project_root"] is None
    assert data["project_source"] == "explicit"


def test_directory_flowstash_is_not_config(api, tmp_path):
    (tmp_path / ".flowstash").unlink()
    (tmp_path / ".flowstash").mkdir()
    assert invoke(["context"]).exit_code == 2


def test_linked_user_from_subdirectory(api, tmp_path, monkeypatch):
    monkeypatch.delenv("FLOWSTASH_API_KEY")
    (tmp_path / ".flowstash").write_text("project_id: p\nlinked_user: linked@example.com\n")
    child = tmp_path / "src"
    child.mkdir()
    monkeypatch.chdir(child)
    monkeypatch.setattr(config, "get_token_for_user", lambda email: "linked-token" if email == "linked@example.com" else None)
    api["token"] = "linked-token"
    api["identity"]["email"] = "linked@example.com"
    result = invoke(["context"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["auth_method"] == "login"


def test_missing_explicit_user_never_falls_back(api, monkeypatch):
    monkeypatch.delenv("FLOWSTASH_API_KEY")
    monkeypatch.setattr(config, "get_token_for_user", lambda email: None)
    result = invoke(["context", "--user", "missing@example.com"])
    assert result.exit_code == 3
    assert not api["requests"]


def test_key_and_user_are_ambiguous(api):
    result = invoke(["context", "--user", "alice"])
    assert result.exit_code == 2
    assert not api["requests"]


@pytest.mark.parametrize("identity,code", [({"tenant_id": "other"}, "TENANT_MISMATCH"), ({"tenant_id": "t", "project_id": "other"}, "PROJECT_RESTRICTION"), ({"tenant_id": "t", "environment": "staging"}, "ENVIRONMENT_RESTRICTION")])
def test_identity_restrictions(api, identity, code):
    api["identity"] = identity
    result = invoke(["context"])
    assert result.stdout == ""
    assert json.loads(result.stderr)["error"]["code"] == code
    assert len(api["requests"]) == 1


def test_key_failure_no_fallback(api, monkeypatch):
    api["auth_status"] = 401
    monkeypatch.setattr(config, "get_token_for_user", lambda email: pytest.fail("Credential fallback"))
    result = invoke(["context"])
    assert result.exit_code == 3
    assert len(api["requests"]) == 1
    assert "secret" not in result.output


def test_context_local_mode_and_diagnostic_rejection(api):
    assert json.loads(invoke(["context", "--env", "dev"]).stdout)["managed"] is False
    assert invoke(["obs", "runs", "--env", "dev"]).exit_code == 2
    assert invoke(["context", "--env", "unknown"]).exit_code == 2


@pytest.mark.parametrize("resource,path,collection", [("runs", "runs", "runs"), ("logs", "logs", "logs"), ("spans", "spans", "spans"), ("io", "data-exchanges", "data_exchanges"), ("records", "records", "records"), ("events", "run-events", "events")])
def test_project_resource_paths_and_defaults(api, resource, path, collection):
    api["result"] = {collection: [], "next_page_token": "opaque"}
    result = invoke(["obs", resource, "--record-key", "custom:a/b ?&=✓", "--operation-id", "execution-op"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == api["result"]
    request = api["requests"][-1]
    assert request.url.path == "/observability/" + path
    assert request.url.params["record_key"] == "custom:a/b ?&=✓"
    assert request.url.params["operation_id"] == "execution-op"
    assert request.url.params["environment"] == "prod"
    assert request.url.params["project_id"] == "p"
    assert request.url.params["limit"] == "50"
    assert request.url.params["order"] == "desc"
    start, end = (datetime.fromisoformat(request.url.params[k]) for k in ("from_time", "to_time"))
    assert end - start == timedelta(hours=24)


@pytest.mark.parametrize("resource,path", [("logs", "logs"), ("spans", "spans"), ("io", "data-exchanges"), ("records", "records"), ("events", "events")])
def test_run_paths_full_run_defaults(api, resource, path):
    result = invoke(["obs", resource, "--run-id", "run/with ?#"])
    assert result.exit_code == 0, result.output
    request = api["requests"][-1]
    assert request.url.raw_path.split(b"?")[0] == b"/observability/runs/run%2Fwith%20%3F%23/" + path.encode()
    assert request.url.params["limit"] == ("100" if resource == "logs" else "50")
    assert request.url.params["order"] == "asc"
    assert "from_time" not in request.url.params
    assert "to_time" not in request.url.params


def test_fixed_window_reused_with_cursor_and_new_limit(api):
    args = ["obs", "logs", "--from", "2026-10-13T02:00:00+02:00", "--to", "2026-10-14T00:00:00Z"]
    assert invoke(args).exit_code == 0
    first = api["requests"][-1].url.params
    assert invoke(args + ["--page-token", "opaque", "--limit", "17"]).exit_code == 0
    second = api["requests"][-1].url.params
    assert first["from_time"] == second["from_time"] == "2026-10-13T00:00:00Z"
    assert first["to_time"] == second["to_time"]
    assert second["page_token"] == "opaque"
    assert second["limit"] == "17"
    assert invoke(["obs", "logs", "--page-token", "opaque"]).exit_code == 2
    assert invoke(args[0:2] + ["--page-token", "opaque", "--since", "2h"]).exit_code == 2


@pytest.mark.parametrize("args", [["--from", "2026-10-14T00:00:00"], ["--since", "0h"], ["--since", "-2h"], ["--since", "1.5h"], ["--since", "2w"], ["--since", "2h", "--from", "2026-10-13T00:00:00Z"], ["--from", "2026-10-14T00:00:00Z", "--to", "2026-10-13T00:00:00Z"], ["--limit", "501"], ["--limit", "0"], ["--limit", "bad"], ["--order", "bad"], ["--state", "FAILED"]])
def test_usage_errors_are_json(api, args):
    result = invoke(["obs", "logs"] + args)
    assert result.exit_code == 2, result.output
    assert result.stdout == ""
    assert "error" in json.loads(result.stderr)
    assert not api["requests"]


def test_since_anchors_to_explicit_end():
    assert time_window(since="2h", to_time="2026-10-14T12:00:00+02:00") == {"from_time": "2026-10-14T08:00:00Z", "to_time": "2026-10-14T10:00:00Z"}
    assert time_window() == {}
    with pytest.raises(DiagnosticError):
        time_window(since="9999999999999999999999999999999d")


@pytest.mark.parametrize("status,exit_code,code", [(400, 2, "INVALID_QUERY"), (401, 3, "AUTH_REQUIRED"), (403, 3, "FORBIDDEN"), (404, 4, "NOT_FOUND"), (422, 2, "INVALID_PARAMETERS"), (502, 5, "PROVIDER_FAILURE")])
def test_http_errors_no_retries_or_stdout(api, status, exit_code, code):
    api["status"] = status
    result = invoke(["obs", "logs"])
    assert result.exit_code == exit_code
    assert result.stdout == ""
    assert json.loads(result.stderr)["error"] == {"code": code, "message": json.loads(result.stderr)["error"]["message"], "http_status": status}
    assert len(api["requests"]) == 3


def test_network_error_no_leak(api, respx_mock):
    respx_mock.reset()
    respx_mock.route().mock(side_effect=httpx.ConnectError("secret-key-do-not-leak"))
    result = invoke(["context"])
    assert result.exit_code == 5
    assert result.stdout == ""
    assert "secret" not in result.stderr
    assert json.loads(result.stderr)["error"]["code"] == "NETWORK_FAILURE"


@pytest.mark.parametrize("command,body", [(["obs", "logs"], {"logs": [{"message": {"$payload_ref": "gs://managed-blobs/t/message"}, "attrs": None}], "next_page_token": "opaque"}), (["obs", "io"], {"data_exchanges": [{"request_payload_ref": "gs://managed-blobs/t/request", "response_payload_ref": "gs://managed-blobs/t/response"}], "next_page_token": None}), (["obs", "records"], {"records": [{"link_id": "l", "source_run_id": "r", "source_span_id": "s", "source_operation_id": "o", "source_feed_id": "f", "causation_event_id": "c", "kind": "SOURCE", "source": "FEED"}], "next_page_token": None}), (["obs", "payload", "gs://managed-blobs/t/a?b&c/#"], {"ref": "gs://managed-blobs/t/a?b&c/#", "content_type": "application/octet-stream", "encoding": "base64", "body": "AAEC", "size_bytes": 3})])
def test_server_shapes_preserved_without_hydration(api, command, body):
    api["result"] = body
    result = invoke(command)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == body
    assert len(api["requests"]) == 3
    if command[1] == "payload":
        assert api["requests"][-1].url.params["ref"] == command[2]
        assert api["requests"][-1].url.params["environment"] == "prod"


def test_single_run_is_context_scoped(api):
    api["result"] = {"run_id": "r", "project_id": "p", "environment": "prod"}
    assert invoke(["obs", "run", "r"]).exit_code == 0
    api["result"]["environment"] = "other"
    result = invoke(["obs", "run", "r"])
    assert result.exit_code == 4
    assert result.stdout == ""


def test_build_list_native_envelope_epoch_times_and_no_environment(api):
    api["result"] = {"builds": [], "next_page_token": "build-page"}
    result = invoke(["builds", "list", "--from", "2026-10-13T00:00:00Z", "--to", "2026-10-14T00:00:00Z", "--limit", "25"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == api["result"]
    req = api["requests"][-1]
    assert req.url.path == "/v1/builds"
    assert set(req.url.params) == {"project_id", "page_size", "start_ts", "end_ts"}
    assert float(req.url.params["end_ts"]) - float(req.url.params["start_ts"]) == 86400
    assert invoke(["builds", "list"]).exit_code == 0
    assert set(api["requests"][-1].url.params) == {"project_id", "page_size"}


def test_build_log_pages_beyond_2000_and_empty_continuation(api):
    for page in range(6):
        api["result"] = {"entries": [{"text": f"line-{page*500+i}"} for i in range(500)], "next_page_token": f"page-{page+1}"}
        args = ["builds", "logs", "managed/build", "--limit", "500", "--severity", "ERROR", "--search", 'quoted "text"']
        if page:
            args += ["--page-token", f"page-{page}"]
        result = invoke(args)
        assert result.exit_code == 0, result.output
        assert len(json.loads(result.stdout)["entries"]) == 500
        req = api["requests"][-1]
        assert req.url.raw_path.split(b"?")[0] == b"/v1/builds/managed%2Fbuild/log-entries"
        assert req.url.params["order"] == "asc"
        assert "from_time" not in req.url.params
        assert "environment" not in req.url.params
    api["result"] = {"entries": [], "next_page_token": "more"}
    assert json.loads(invoke(["builds", "logs", "b"]).stdout) == api["result"]


def test_build_cannot_escape_project(api):
    api["build"] = {"project_id": "other"}
    assert invoke(["builds", "get", "b"]).exit_code == 4
    assert invoke(["builds", "logs", "b"]).exit_code == 4
    assert not any(r.url.path.endswith("/log-entries") for r in api["requests"])


def test_human_pagination_has_reusable_window(api):
    api["result"]["next_page_token"] = "opaque"
    result = runner.invoke(app, ["obs", "logs"])
    assert result.exit_code == 0
    assert "Time range: --from " in result.stdout
    assert "--to " in result.stdout
    assert "--page-token opaque" in result.stdout


@pytest.mark.parametrize("name", ["run", "build", "login", "deploy", "client"])
def test_existing_command_help_still_available(api, name):
    assert runner.invoke(app, [name, "--help"]).exit_code == 0
    assert not api["requests"]


def test_explicit_user_identity_mismatch(api, monkeypatch):
    monkeypatch.delenv("FLOWSTASH_API_KEY")
    monkeypatch.setattr(config, "get_token_for_user", lambda email: "wrong-user-token")
    api["token"] = "wrong-user-token"
    api["identity"]["email"] = "wrong@example.com"
    result = invoke(["context", "--user", "requested@example.com"])
    assert result.exit_code == 3
    assert json.loads(result.stderr)["error"]["code"] == "IDENTITY_MISMATCH"
    assert len(api["requests"]) == 1


@pytest.mark.parametrize("args", [["obs", "run"], ["obs", "payload"], ["builds", "get"], ["builds", "logs"], ["obs", "unknown"], ["obs", "events", "--event-type", "WRONG"], ["builds", "logs", "b", "--severity", "WRONG"]])
def test_click_failures_machine_readable(api, args):
    result = invoke(args)
    assert result.exit_code == 2, result.output
    assert result.stdout == ""
    assert json.loads(result.stderr)["error"]["code"] == "INVALID_PARAMETERS"


@pytest.mark.parametrize("scope,project,env,expected,write_target", [
    ("observability:read", "p", "prod", {"project_id": "p", "environment": "prod"}, None),
    ("observability:read", None, "prod", {"project_id": "p", "environment": "prod"}, None),
    ("observability:read", None, None, {}, None),
    ("observability:ingest", None, "prod", {}, "prod"),
    ("observability:ingest", "p", "prod", {"project_id": "p", "environment": "prod"}, "prod"),
])
def test_key_creation_scope_restrictions_and_legacy_env(api, monkeypatch, scope, project, env, expected, write_target):
    from flowstash.cli.commands import apikey
    monkeypatch.setattr(apikey, "resolve_credentials", lambda user=None: "developer")
    monkeypatch.setattr(apikey, "_write_key_to_env", lambda env, key: writes.append((env, key)))
    writes = []
    api["token"] = "developer"
    api["result"] = {"api_key": "one-time-key", "key_id": "key-id"}
    args = ["api-keys", "new", "--label", "Workspace agent", "--scope", scope]
    if project:
        args += ["--project", project]
    if env:
        args += ["--env", env]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    req = api["requests"][-1]
    assert req.method == "POST"
    assert req.url.path == "/v1/api-keys"
    assert json.loads(req.content) == {"label": "Workspace agent", "scopes": [scope], **expected}
    assert writes == ([(write_target, "one-time-key")] if write_target else [])


def test_key_creation_explicit_write_env_and_alias(api, monkeypatch):
    from flowstash.cli.commands import apikey
    monkeypatch.setattr(apikey, "resolve_credentials", lambda user=None: "developer")
    writes = []
    monkeypatch.setattr(apikey, "_write_key_to_env", lambda env, key: writes.append((env, key)))
    api["token"] = "developer"
    api["result"] = {"api_key": "one-time-key", "key_id": "key-id"}
    result = runner.invoke(app, ["api-keys", "create", "--label", "agent", "--scope", "observability:read", "--project", "p", "--env", "prod", "--write-env", "prod"])
    assert result.exit_code == 0, result.output
    assert writes == [("prod", "one-time-key")]


def test_key_environment_requires_project(api, tmp_path, monkeypatch):
    from flowstash.cli.commands import apikey
    monkeypatch.setattr(apikey, "resolve_credentials", lambda user=None: "developer")
    (tmp_path / ".flowstash").unlink()
    result = runner.invoke(app, ["api-keys", "new", "--label", "agent", "--scope", "observability:read", "--env", "prod"])
    assert result.exit_code == 2
    assert not api["requests"]


def test_restricted_key_build_reads_allowed(api):
    api["identity"] = {"tenant_id": "t", "project_id": "p", "environment": "prod", "scopes": ["observability:read"]}
    assert invoke(["builds", "get", "b"]).exit_code == 0


def test_current_and_legacy_credentials_preserved(api, monkeypatch, tmp_path):
    monkeypatch.delenv("FLOWSTASH_API_KEY")
    monkeypatch.setattr(config, "load_global_config", lambda: config.GlobalConfig(current_user="current@example.com"))
    monkeypatch.setattr(config, "get_token_for_user", lambda email: "current-token" if email == "current@example.com" else None)
    api["token"] = "current-token"
    assert invoke(["context"]).exit_code == 0
    monkeypatch.setattr(config, "get_token_for_user", lambda email: None)
    monkeypatch.setattr(config.keyring, "get_password", lambda service, key: "legacy-token")
    api["token"] = "legacy-token"
    assert invoke(["context"]).exit_code == 0


def test_config_explicit_path_and_roundtrip(tmp_path, monkeypatch):
    original = config.ProjectConfig(project_id="p", linked_user="u", environments=[config.EnvironmentMode(name="prod", managed=True)])
    monkeypatch.chdir(tmp_path)
    config.save_project_config(original)
    assert config.load_project_config(tmp_path) == original
    assert config.load_project_config(tmp_path / ".flowstash") == original

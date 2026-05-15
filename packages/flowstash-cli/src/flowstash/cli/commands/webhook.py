import os
import sys
import json
import time
import base64
import asyncio
import datetime
import difflib
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any

import typer
import httpx
import websockets
import questionary
from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.table import Table
from rich.panel import Panel

from ..core.config import get_access_token, load_project_config, load_global_config
from ..core.patcher import apply_patch

app = typer.Typer()
console = Console()


def get_api_url() -> str:
    global_config = load_global_config()
    return (
        getattr(global_config, "api_url", "https://api.flowstash.dev")
        if global_config
        else os.getenv("FLOWSTASH_API_URL", "https://api.flowstash.dev")
    )


def _build_target_url(target: str, path: str) -> str:
    return f"{target.rstrip('/')}/{path.lstrip('/')}"


def _get_webhooks_from_app(entry: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Loads the app entrypoint in a subprocess and extracts registered webhooks."""

    debug_flag = "True" if debug else "False"

    script = f"""
import sys
import json
import importlib
import traceback
import os
sys.path.insert(0, ".")
_src = os.path.join(os.getcwd(), "src")
if os.path.isdir(_src) and _src not in sys.path:
    sys.path.insert(1, _src)

_debug = {debug_flag}

if _debug:
    print(json.dumps({{"__debug__": True, "python": sys.executable, "cwd": os.getcwd(), "sys_path": sys.path}}))

try:
    module = importlib.import_module("{entry.replace('.py', '')}")
except Exception as e:
    tb = traceback.format_exc()
    print(json.dumps({{"error": str(e), "traceback": tb}}))
    sys.exit(1)

from flowstash.ingress import ingress

# Try to see if the module has a FastAPI app instance with routes
app_instance = getattr(module, "app", None)
mapped_endpoints = {{}}
if app_instance and hasattr(app_instance, "routes"):
    for route in app_instance.routes:
        ep = getattr(route, "endpoint", None)
        if ep and hasattr(ep, "_ingress_metadata"):
            original_path = ep._ingress_metadata.get("path")
            if original_path:
                mapped_endpoints[original_path] = getattr(route, "path", original_path)

result = []
for h in ingress.get_webhooks():
    m = h._ingress_metadata
    
    actual_path = mapped_endpoints.get(m["path"], m["path"])
    
    tp = m.get("test_payload")
    tp_path = getattr(tp, "path", None) if tp else None
    result.append({{
        "path": actual_path, 
        "method": m.get("method", "POST"), 
        "source_locator": m.get("source_locator"),
        "test_payload_path": tp_path
    }})

print(json.dumps({{"webhooks": result}}))
    """

    try:
        res = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True
        )

        stdout_lines = [l for l in res.stdout.splitlines() if l.strip()]

        debug_info = None
        result_data = None
        for line in stdout_lines:
            try:
                parsed = json.loads(line)
                if "__debug__" in parsed:
                    debug_info = parsed
                elif "error" in parsed or "webhooks" in parsed:
                    result_data = parsed
            except json.JSONDecodeError:
                if debug:
                    console.print(f"[dim]stdout (non-JSON):[/dim] {escape(line)}")

        if debug:
            if debug_info:
                console.print(f"[bold]Debug info:[/bold]")
                console.print(f"  [dim]Python:[/dim]  {debug_info.get('python')}")
                console.print(f"  [dim]CWD:   [/dim]  {debug_info.get('cwd')}")
                console.print(f"  [dim]entry: [/dim]  {entry}")
                console.print(f"  [dim]sys.path:[/dim]")
                for p in debug_info.get("sys_path", []):
                    console.print(f"    [dim]{p}[/dim]")
            if res.stderr:
                console.print(
                    f"[bold]stderr:[/bold]\n[dim]{escape(res.stderr.strip())}[/dim]"
                )

        if result_data is None:
            console.print("[red]No JSON result received from subprocess.[/red]")
            if not debug and res.stderr:
                console.print(f"[dim]{res.stderr.strip()}[/dim]")
            raise typer.Exit(code=1)

        if "error" in result_data:
            console.print(
                f"[red]Error loading app:[/red] {escape(result_data['error'])}"
            )
            if result_data.get("traceback"):
                if debug:
                    console.print(
                        f"[dim]{escape(result_data['traceback'].strip())}[/dim]"
                    )
                elif res.stderr:
                    console.print(f"[dim]{escape(res.stderr.strip())}[/dim]")
            raise typer.Exit(code=1)

        return result_data.get("webhooks", [])
    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.strip() if e.stderr else ""
        stdout_output = e.stdout.strip() if e.stdout else ""
        console.print(f"[red]Failed to extract webhooks from {entry}[/red]")
        if stderr_output:
            snippet = stderr_output if debug else stderr_output[:500]
            console.print(f"[dim]stderr:[/dim] {escape(snippet)}")
        if stdout_output:
            snippet = stdout_output if debug else stdout_output[:500]
            console.print(f"[dim]stdout:[/dim] {escape(snippet)}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(
            f"[red]Failed to extract webhooks from {entry}:[/red] {escape(str(e))}"
        )
        raise typer.Exit(code=1)


async def _listen_stream(
    ws_url: str,
    captures_buffer: List[Dict[str, Any]],
    stop_event: asyncio.Event,
    error_ref: Optional[List[str]] = None,
):
    """Connects to the WebSocket and appends captures to the buffer."""

    # Convert http/https to ws/wss
    ws_base = ws_url.replace("http://", "ws://").replace("https://", "wss://")

    try:
        async with websockets.connect(ws_base) as ws:
            # console.print(f"[dim]Connected to {ws_base}[/dim]")
            while not stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(message)
                    captures_buffer.append(data)
                    # Keep ring buffer at 50 max
                    if len(captures_buffer) > 50:
                        captures_buffer.pop(0)
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    if error_ref is not None:
                        error_ref.append("Connection closed by server.")
                    stop_event.set()
                    break
    except Exception as e:
        if error_ref is not None:
            error_ref.append(str(e))
        stop_event.set()


async def _wait_for_keypress(stop_event: asyncio.Event):
    """Wait for user to press any key (Enter in this case to be standard)."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, input)
    stop_event.set()


async def _run_listen_ui(ws_url: str, path: str, ingest_url: str):
    captures_buffer: List[Dict[str, Any]] = []
    stop_event = asyncio.Event()
    ws_error: List[str] = []

    # Start WS listener task
    ws_task = asyncio.create_task(
        _listen_stream(ws_url, captures_buffer, stop_event, ws_error)
    )
    input_task = asyncio.create_task(_wait_for_keypress(stop_event))

    console.print(f"[green]Listening on[/green] [bold]{ingest_url}[/bold]")
    console.print("Press [bold]Enter[/bold] to stop.\n")

    def _build_table() -> Panel:
        table = Table(
            show_header=True,
            header_style="bold cyan",
            box=None,
            expand=True,
            padding=(0, 1),
        )
        table.add_column("#", style="dim", width=4, no_wrap=True)
        table.add_column("Received", width=14, no_wrap=True)
        table.add_column("Age", width=10, no_wrap=True)
        table.add_column("Size", width=10, no_wrap=True)

        if not captures_buffer:
            table.add_row("—", "—", "—", "[dim]waiting for first event...[/dim]")
        else:
            for i, c in enumerate(reversed(captures_buffer[-10:])):  # Show last 10
                seq = len(captures_buffer) - i
                ts = c.get("captured_at", time.time())
                diff = int(time.time() - ts)
                age = f"{diff}s ago" if diff < 60 else f"{diff // 60}m ago"
                # CapturedPayload structure: parsed.json_data / parsed.text_data / request.raw_body_b64
                parsed = c.get("parsed") or {}
                body = parsed.get("json_data") or parsed.get("text_data") or ""
                if body:
                    size = (
                        len(json.dumps(body))
                        if not isinstance(body, str)
                        else len(body)
                    )
                else:
                    # fallback: base64-decoded length of raw body
                    raw_b64 = (c.get("request") or {}).get("raw_body_b64", "")
                    size = len(base64.b64decode(raw_b64)) if raw_b64 else 0
                dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                received_str = dt.strftime("%H:%M:%S")
                table.add_row(
                    f"[bold]{seq}[/bold]",
                    received_str,
                    f"[green]{age}[/green]",
                    f"{size} B",
                )

        total = len(captures_buffer)
        title = (
            f"[bold]Events captured: {total}[/bold]"
            if total
            else "[dim]No events yet[/dim]"
        )
        return Panel(table, title=title, border_style="green" if total else "dim")

    with Live(_build_table(), refresh_per_second=4, vertical_overflow="crop") as live:
        while not stop_event.is_set():
            live.update(_build_table())
            await asyncio.sleep(0.25)

    # Wait for tasks to clean up
    ws_task.cancel()
    input_task.cancel()

    if ws_error:
        console.print(f"[red]WebSocket connection error:[/red] {ws_error[0]}")
        return []

    return captures_buffer


@app.command()
def listen(
    entry: str = typer.Option(
        "api_main.py",
        "--entry",
        "-e",
        help=(
            "Path to your application entrypoint (Python file). "
            "This file must import your flowstash app so that registered webhooks can be discovered. "
            "Example: --entry app.py"
        ),
        show_default=True,
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Print debug info: Python executable, CWD, sys.path, import errors, and raw stderr.",
    ),
):
    """
    [bold green]Listen[/bold green] for real webhook payloads and save them as local test fixtures.

    This command will:

    [bold]1.[/bold] Scan your app entrypoint ([bold cyan]--entry[/bold cyan]) for registered [bold]@ingress.webhook[/bold] handlers.
    [bold]2.[/bold] Let you pick which webhook path to listen on.
    [bold]3.[/bold] Create a temporary ingest URL you can send real HTTP requests to.
    [bold]4.[/bold] Stream captured payloads in real time over a WebSocket.
    [bold]5.[/bold] Save the selected payload as a JSON fixture (e.g. [dim]tests/payloads/webhooks/...[/dim]).
    [bold]6.[/bold] Optionally patch your [bold]@ingress.webhook[/bold] decorator with [bold]test_payload=FromFile(...)[/bold].

    [yellow]Examples:[/yellow]
      [bold]flowstash webhook listen[/bold]
      [bold]flowstash webhook listen --entry src/app.py[/bold]

    [dim]Requires:[/dim] login ([bold]flowstash login[/bold]) and an initialized project ([bold]flowstash init[/bold]).
    """
    token = get_access_token()
    if not token:
        console.print("[red]Not logged in. Run 'flowstash login' first.[/red]")
        raise typer.Exit(code=1)

    project_config = load_project_config()
    if not project_config:
        console.print(
            "[red]No .flowstash found. Please run 'flowstash init' first.[/red]"
        )
        raise typer.Exit(code=1)

    project_id = project_config.project_id
    if not project_id:
        console.print("[red]Project is not connected.[/red]")
        console.print(
            "Please connect to a project first using [bold]flowstash link[/bold]."
        )
        raise typer.Exit(code=1)

    api_url = get_api_url()

    console.print(f"Loaded app from: [bold]{entry}[/bold] (app variable found)")

    webhooks = _get_webhooks_from_app(entry, debug=debug)
    if not webhooks:
        console.print("[yellow]No webhooks found ([@ingress.webhook]).[/yellow]")
        raise typer.Exit(code=0)

    # Prompt user to select webhook
    choices = [w["path"] for w in webhooks]
    selected_path = questionary.select(
        "Select webhook to listen on:", choices=choices
    ).ask()

    if not selected_path:
        raise typer.Exit(code=0)

    selected_webhook = next(w for w in webhooks if w["path"] == selected_path)

    # Start session on backend
    with httpx.Client() as client:
        resp = client.post(
            f"{api_url}/cli/webhooks/sessions",
            headers={"Authorization": f"Bearer {token}"},
            json={"path": selected_path, "project_id": project_id},
        )
        if resp.status_code != 200:
            console.print(f"[red]Failed to start session:[/red] {resp.text}")
            raise typer.Exit(code=1)

        session = resp.json()

    full_ingest_url = f"{api_url}{session['ingest_url']}"
    full_ws_url = f"{api_url}{session['ws_url']}"

    # Run async UI
    captures = asyncio.run(_run_listen_ui(full_ws_url, selected_path, full_ingest_url))

    if not captures:
        console.print("No payloads captured.")
        raise typer.Exit(code=0)

    # Ask to select payload to review
    import datetime

    capture_choices = []
    for i, c in enumerate(reversed(captures)):
        ts = c.get("captured_at", time.time())
        diff = int(time.time() - ts)
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        dt_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        label = f"{i + 1}) {diff}s ago [{dt_str}]"
        capture_choices.append(questionary.Choice(title=label, value=c))

    selected_capture = questionary.select(
        "Select payload to review:", choices=capture_choices
    ).ask()

    if not selected_capture:
        raise typer.Exit(code=0)

    # Save fixture to file
    ts = selected_capture.get("captured_at", time.time())
    dt_str = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H-%M-%SZ"
    )

    slug = selected_path.strip("/").replace("/", "_")
    if not slug:
        slug = "root"

    default_path = f"tests/payloads/webhooks/{slug}/{dt_str}.json"

    save_path = questionary.text(
        "Save fixture to file (default shown):", default=default_path
    ).ask()

    if not save_path:
        save_path = default_path

    # Write file
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Prepare fixture object
    req = selected_capture.get("request") or {}
    parsed = selected_capture.get("parsed") or {}

    body = parsed.get("json_data")
    if body is None:
        body = parsed.get("text_data")
    if body is None:
        body = req.get("raw_body_b64")

    fixture = {
        "method": req.get("method"),
        "path": req.get("path"),
        "headers": req.get("headers"),
        "query": req.get("query"),
        "body": body,
        "content_type": req.get("content_type"),
    }

    with open(save_path, "w") as f:
        json.dump(fixture, f, indent=2)

    console.print(f"[green]Saved:[/green] {save_path}")

    # Ask to annotate
    do_annotate = questionary.confirm(
        "Annotate webhook decorator with this fixture?", default=True
    ).ask()

    if not do_annotate:
        console.print("Done.")
        raise typer.Exit(code=0)

    # Identify source_locator
    loc = selected_webhook.get("source_locator")
    if not loc or not loc.get("file"):
        console.print("[yellow]Could not determine source file automatically.[/yellow]")
        console.print("Add this to your decorator:")
        console.print(f'  test_payload=FromFile("{save_path}")')
        raise typer.Exit(code=0)

    target_file = loc["file"]
    func_name = loc["name"]
    start_line = loc["line"]

    if not os.path.exists(target_file):
        console.print(f"[red]Source file not found:[/red] {target_file}")
        raise typer.Exit(code=1)

    with open(target_file, "r") as f:
        source_code = f.read()

    new_source = apply_patch(
        source_code, selected_path, save_path, func_name, start_line
    )

    if new_source == source_code:
        console.print(
            "[yellow]No changes made. Could not find exact decorator to patch.[/yellow]"
        )
        console.print("Add this to your decorator manually:")
        console.print(f'  test_payload=FromFile("{save_path}")')
        raise typer.Exit(code=0)

    # Show git-style unified diff
    from rich.syntax import Syntax

    diff_lines = list(
        difflib.unified_diff(
            source_code.splitlines(keepends=True),
            new_source.splitlines(keepends=True),
            fromfile=f"a/{os.path.relpath(target_file)}",
            tofile=f"b/{os.path.relpath(target_file)}",
            lineterm="",
        )
    )
    diff_text = "".join(diff_lines)

    console.print()
    console.print(Syntax(diff_text, "diff", theme="monokai", line_numbers=False))
    console.print()

    apply_patch_confirm = questionary.confirm("Apply patch?", default=True).ask()

    if apply_patch_confirm:
        with open(target_file, "w") as f:
            f.write(new_source)
        console.print(f"[green]Updated:[/green] {target_file}")

    console.print("Done.")


@app.command()
def test(
    entry: str = typer.Option(
        "api_main.py",
        "--entry",
        "-e",
        help="Path to your application entrypoint (Python file).",
        show_default=True,
    ),
    path: Optional[str] = typer.Option(
        None,
        "--path",
        help="The webhook path to test.",
    ),
    target: str = typer.Option(
        "http://localhost:8000",
        "--target",
        help="The target URL base where the API is running.",
        show_default=True,
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Print debug info: Python executable, CWD, sys.path, import errors, and raw stderr.",
    ),
):
    """
    [bold green]Test[/bold green] a webhook by sending its test payload to the local API.

    This command will:
    [bold]1.[/bold] Load the registered webhooks from your app entrypoint.
    [bold]2.[/bold] Find the [bold]test_payload[/bold] fixture for the selected webhook path.
    [bold]3.[/bold] Send the fixture payload to the specified target URL.
    """
    webhooks = _get_webhooks_from_app(entry, debug=debug)
    if not webhooks:
        console.print("[yellow]No webhooks found ([@ingress.webhook]).[/yellow]")
        if not debug:
            console.print(
                "[dim]Tip: run with --debug for detailed import diagnostics.[/dim]"
            )
        raise typer.Exit(code=0)

    actual_path = path
    prompt_for_target_url = False
    selected_webhook = None

    if actual_path:
        selected_webhook = next((w for w in webhooks if w["path"] == actual_path), None)
        if not selected_webhook:
            matches = [w for w in webhooks if actual_path.endswith(w["path"])]
            if len(matches) == 1:
                selected_webhook = matches[0]

    if not selected_webhook:
        if actual_path:
            console.print(
                f"[dim]Could not automatically resolve payload for '{actual_path}'.[/dim]"
            )

        choices = [w["path"] for w in webhooks]
        prompt = (
            f"Select webhook payload to send to '{actual_path}':"
            if actual_path
            else "Select webhook to test:"
        )
        webhook_path = questionary.select(prompt, choices=choices).ask()

        if not webhook_path:
            raise typer.Exit(code=0)

        prompt_for_target_url = True
        selected_webhook = next(
            (w for w in webhooks if w["path"] == webhook_path), None
        )

        if not actual_path:
            actual_path = webhook_path

    test_payload_path = selected_webhook.get("test_payload_path")
    if not test_payload_path:
        hook_path = selected_webhook.get("path")
        console.print(f"[red]No test_payload defined for {hook_path}.[/red]")
        console.print(
            "Please use 'flowstash webhook listen' first to capture a payload and annotate your decorator."
        )
        raise typer.Exit(code=1)

    if not os.path.exists(test_payload_path):
        console.print(f"[red]Test payload file not found:[/red] {test_payload_path}")
        raise typer.Exit(code=1)

    with open(test_payload_path, "r") as f:
        try:
            fixture = json.load(f)
        except json.JSONDecodeError as e:
            console.print(f"[red]Failed to parse fixture JSON:[/red] {e}")
            raise typer.Exit(code=1)

    # Reconstruct request
    method = fixture.get("method") or selected_webhook.get("method", "POST")
    headers = fixture.get("headers") or {}
    query = fixture.get("query") or {}
    body = fixture.get("body")

    # Strip calculated headers such as Content-Length and Host
    if isinstance(headers, dict):
        headers = {
            k: v
            for k, v in headers.items()
            if k.lower() not in ("content-length", "host", "connection")
        }

    target_url = _build_target_url(target, actual_path)

    if prompt_for_target_url:
        target_url = questionary.text(
            "Target URL to call (modify base URL, port, or path):",
            default=target_url,
        ).ask()
        if not target_url:
            raise typer.Exit(code=0)

    console.print(f"[cyan]Testing {method} {target_url}...[/cyan]")

    with httpx.Client() as client:
        try:
            kwargs = {}
            if isinstance(body, (dict, list)):
                kwargs["json"] = body
            elif body is not None:
                kwargs["content"] = body

            resp = client.request(
                method, target_url, headers=headers, params=query, **kwargs
            )

            if resp.status_code < 400:
                console.print(f"Status: [bold green]{resp.status_code}[/bold green]")
            else:
                console.print(f"Status: [bold red]{resp.status_code}[/bold red]")

            try:
                console.print_json(resp.text)
            except Exception:
                console.print(resp.text)

        except httpx.RequestError as exc:
            console.print(
                f"[red]An error occurred while requesting {exc.request.url!r}.[/red]"
            )
            raise typer.Exit(code=1)

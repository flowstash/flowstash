# Managed Backend

Managed workers support two execution modes.

## 1. Feed Consumer Mode

Default mode when the worker starts without a managed job command.

- Entry path: `run_worker(config)`
- Behavior: starts the FastAPI server from `main.py`
- Use for: feed delivery and managed HTTP callbacks

Typical container entrypoint:

```bash
python -u worker_main.py
```

This exposes the managed HTTP endpoints, including:

- `POST /handle_task`
- `POST /internal/feed/kick/batched`
- `POST /internal/feed/deliver/classic`

## 2. Cloud Job Mode

Activated when the first CLI argument is one of:

- `run-task`
- `register_schedules`
- `consume-feed`
- `consume-feed-batch`

Use for one-shot job execution. The process runs the command and exits.

Examples:

```bash
python -u worker_main.py run-task my_module.my_task 123 --dry_run true
python -u worker_main.py register_schedules <deploy_id>
python -u worker_main.py consume-feed <base64_envelope>
python -u worker_main.py consume-feed-batch <batch_id>
```

## Mode Selection Rule

- No managed command: start HTTP server
- Managed command present: run one-shot job path and exit

This split is implemented in `runner.py` via `is_managed_job_mode()`.

# Flowstash diagnostic command filters

Every diagnostic command supports `--json`, `--project ID`, `--env ENV` (default prod), and `--user EMAIL`. `context` reports validated identity/location; `obs run RUN_ID` returns one summary and `obs payload REF [--run-id RUN_ID]` fetches one body.

Query commands support `--limit` (1–500), `--page-token`, `--order asc|desc`, `--from`, `--to`, and `--since`. All observability searches support `--integration`, `--pipeline`, `--run-id`, `--span-id`, `--trace-id`, `--parent-run-id`, `--operation-id`, and `--record-key`.

| Command | Additional flags | JSON collection |
|---|---|---|
| `obs runs` | `--status` | `runs` |
| `obs spans` | `--status`, `--search` (span name substring) | `spans` |
| `obs logs` | `--severity`, `--search` (literal case-insensitive message substring) | `logs` |
| `obs io` | `--state`, `--remote-system`, `--channel`, `--operation`, `--status-code`, `--query` (address/remote-system substring) | `data_exchanges` |
| `obs records` | `--kind`, `--source` | `records` |
| `obs events` | `--status`, `--event-type STARTED|PROGRESS|ENDED|SCHEDULED` | `events` |
| `builds list` | optional time bounds; server descending order | `builds` |
| `builds logs BUILD_ID` | `--severity`, `--search` (Cloud Logging quoted text search) | `entries` |

Build log severities: DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY. `builds get BUILD_ID` returns one status object. Build IDs must be managed IDs, not native GCP IDs.

First project-wide observation: 24-hour default window, descending, 50 items. Run scope: complete run, ascending, 50 items (100 for logs). Build logs: complete build, ascending, 50. Build lists: optional bounds, descending, 50. Query results include `next_page_token`; fetch another page deliberately. A continuation of a bounded project search requires the exact resolved `--from` and `--to`; do not recompute relative times.

`--json` emits one unchanged server value to stdout. Errors leave stdout empty and emit `{"error":{"code":"...","message":"...","http_status":401}}` on stderr, with HTTP status omitted for local errors. Exit codes: 0 success including empty results; 2 usage/context; 3 authentication/authorization; 4 missing/inaccessible; 5 network/provider/client failure.

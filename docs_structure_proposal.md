# FlowStash Documentation — Structure Proposal

Goal: docs at the level of a premium open-source project (think Prefect / Dagster / Temporal quality), while staying maintainable by a small team.

## Tooling recommendation

**MkDocs + Material theme + mkdocstrings[python]**, living in `docs/` and built via CI to GitHub Pages.

- Material for MkDocs is the de-facto standard for polished Python OSS docs (tabs, admonitions, dark mode, search, versioning via `mike`).
- `mkdocstrings` generates the API reference straight from docstrings — one source of truth, no drift.
- Everything stays Markdown, so docs are reviewable in PRs like code.

(Alternative: Docusaurus — better if a marketing-style landing page matters more than Python API reference. Not recommended here.)

## README.md (repo front door — short, sells the project)

1. Logo / name + one-line pitch ("A managed integration framework for Python")
2. Badges (PyPI, CI, license, Python versions)
3. **What is FlowStash?** — 3–4 sentences + one architecture diagram
4. **Quickstart** — install → define a task → run locally (under 30 lines of code)
5. **Key features** — bullet list (tasks & steps, typed API clients, feeds, scheduling, webhooks, observability, managed cloud deployment)
6. **Package map** — the 5-package table with one-liners
7. Links: full docs, examples, contributing, license

Per-package `README.md` stubs (required for PyPI pages): one paragraph + link to the docs site.

## docs/ site structure

```
docs/
├─ index.md                     # What is FlowStash, who it's for, feature tour
│
├─ getting-started/
│  ├─ installation.md           # pip/uv install, which package to pick (meta vs à la carte)
│  ├─ quickstart.md             # first integration task, run locally, see the result
│  └─ project-setup.md          # flowstash project init, project layout, config files
│
├─ concepts/                    # the mental model — no how-to noise
│  ├─ architecture.md           # packages, dependency graph, runtime topology diagram
│  ├─ tasks-and-steps.md        # @integration_task vs @integration_step, lifecycle
│  ├─ context.md                # execution context, correlation, run identity
│  ├─ clients.md                # client registry, HTTP/GraphQL/OData, auth model
│  ├─ feeds-and-pipelines.md    # record feeds, consumers, serialization, debouncing
│  ├─ state.md                  # integration state stores, persistence model
│  ├─ scheduling-and-ingress.md # schedules, webhooks, queue-based triggering
│  └─ observability.md          # runs, spans, record links, ingestion, stores
│
├─ guides/                      # task-oriented how-tos, each self-contained
│  ├─ building-an-integration.md    # end-to-end walkthrough (the flagship guide)
│  ├─ calling-external-apis.md      # defining clients, auth (OAuth2, API keys), retries
│  ├─ working-with-feeds.md         # producing/consuming records, replays
│  ├─ scheduling-tasks.md
│  ├─ handling-webhooks.md
│  ├─ managing-secrets-and-config.md
│  ├─ testing-integrations.md       # pytest patterns, respx, local runs
│  └─ monitoring-and-debugging.md   # reading runs/spans, common failure modes
│
├─ deployment/
│  ├─ overview.md               # local vs dramatiq vs managed — decision guide
│  ├─ local-development.md
│  ├─ dramatiq-backend.md       # Redis, workers, scaling
│  ├─ managed-gcp.md            # Cloud Run, Cloud Tasks, Scheduler, Firestore; profiles & leases
│  └─ ci-cd.md                  # flowstash build/deploy in pipelines
│
├─ reference/
│  ├─ cli.md                    # every command: project, run, build, deploy, auth, apikey, client, webhook
│  ├─ configuration.md          # all config/env options in one table
│  └─ api/                      # auto-generated via mkdocstrings, one page per package
│     ├─ flowstash-lib.md
│     ├─ flowstash-clients.md
│     ├─ flowstash-runtime.md
│     └─ flowstash-cli.md
│
├─ examples/                    # index page linking to runnable examples in examples/ dir
│  └─ index.md
│
└─ community/
   ├─ contributing.md           # dev setup (Makefile), monorepo workflow, release process
   ├─ changelog.md              # (or link to GitHub releases)
   └─ faq.md
```

Plus repo-root standards for OSS credibility: `LICENSE`, `CONTRIBUTING.md` (can point to docs), `CODE_OF_CONDUCT.md`, `SECURITY.md`, issue/PR templates.

## Principles

- **Diátaxis split**: `getting-started` (tutorial) / `guides` (how-to) / `concepts` (explanation) / `reference` (facts). Never mix — it's what separates premium docs from a wiki dump.
- **Quickstart in under 5 minutes** is the single highest-value page; invest there first.
- Every concept page ends with "→ related guides / reference" links.
- API reference is generated, never hand-written.
- The `*_plan.md` files currently in repo root are internal working notes — move to an untracked `notes/` dir or delete; they shouldn't ship in an OSS repo.

## Suggested build order

1. README + quickstart + installation (the front door)
2. concepts/architecture + tasks-and-steps + clients (the mental model)
3. flagship guide: building-an-integration end-to-end
4. deployment/managed-gcp + CLI reference (the differentiator)
5. remaining guides, generated API reference, community pages

## Open questions before writing

- Is the managed/GCP deployment part of the open-source offering, or a hosted product? (Determines whether `deployment/managed-gcp.md` is a guide or a product page.)
- License choice — needed before anything ships as "open source".
- Do runnable examples exist (a `demo/` is referenced in pytest config) that we can promote into an `examples/` directory?

# CI/CD

Automating test, build, and deploy for FlowStash projects.

## Authenticating in CI

The CLI normally stores tokens in the OS keyring after a browser login — not available in CI. Use the non-interactive path:

```bash
flowstash login -u "$FLOWSTASH_USER" -p "$FLOWSTASH_PASSWORD"
```

and select the account explicitly in subsequent commands with `--user` or the `FLOWSTASH_USER` environment variable. Store the credentials as CI secrets.

## Managed pipeline (GitHub Actions example)

Builds run in the cloud, so the CI runner needs no Docker:

```yaml
name: deploy
on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[api,worker,dev]"
      - run: pytest

  deploy:
    needs: test
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install flowstash
      - run: flowstash login -u "${{ secrets.FLOWSTASH_USER }}" -p "${{ secrets.FLOWSTASH_PASSWORD }}"
      - run: flowstash deploy prod --yes
```

`--yes` skips interactive confirmations. To separate build from rollout (e.g. deploy on tag only), run `flowstash build prod --tag "$GITHUB_SHA"` first and later `flowstash deploy prod --artifact <id> --yes`.

Secrets for the app itself (client credentials, API keys) are **not** baked into the build — they're environment variables on the deployed services, referenced from config as `${VAR}`. See [Secrets & Configuration](../guides/managing-secrets-and-config.md).

## Self-hosted pipeline (Dramatiq backend)

A conventional container pipeline — build the two images, push, roll out:

```yaml
  build-and-push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - run: |
          docker build -f deployment/shared/api.Dockerfile    -t ghcr.io/acme/myapp-api:${{ github.sha }} .
          docker build -f deployment/shared/worker.Dockerfile -t ghcr.io/acme/myapp-worker:${{ github.sha }} .
          docker push ghcr.io/acme/myapp-api:${{ github.sha }}
          docker push ghcr.io/acme/myapp-worker:${{ github.sha }}
```

Then deploy with your orchestrator of choice, providing `REDIS_URL` and `ENVIRONMENT`. See [Dramatiq Backend](dramatiq-backend.md) for the runtime topology.

## Smoke-testing config in CI

Boot the app against the special `SMOKE-TEST` environment to verify that imports and wiring succeed without any real config or credentials:

```bash
ENVIRONMENT=SMOKE-TEST python -c "
from flowstash.config.env_loader import load_config_dir
from flowstash.runtime.ingress.app import create_fastapi_app
from pathlib import Path
create_fastapi_app(load_config_dir('config', environment='SMOKE-TEST'),
                   auto_import=[Path('src/api/routes')])
"
```

A broken decorator, missing import, or invalid client YAML fails this step long before deploy.

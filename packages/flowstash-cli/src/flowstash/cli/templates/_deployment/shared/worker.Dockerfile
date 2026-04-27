# Worker Dockerfile for {project_name}
FROM flowstash/flowstash-base:latest

WORKDIR /app
COPY . .

# Build arg that chooses which extra(s) to install
# e.g. SERVICE_EXTRAS="api" or "worker" or "api,worker"
ARG SERVICE_EXTRAS="worker"

# Install base deps + extras into system site-packages
# Base deps:
RUN uv pip install --system -r pyproject.toml
RUN uv pip install --system -r pyproject.toml --extra worker || echo "no worker extra; skipping"

RUN rm -rf src/api
ENV PYTHONPATH=/app/src:/app
CMD ["python", "-u", "worker_main.py"]

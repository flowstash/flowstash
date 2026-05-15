# API Dockerfile for {project_name}
FROM flowstash/flowstash-base:latest

WORKDIR /app
COPY . .

# Build arg that chooses which extra(s) to install
# e.g. SERVICE_EXTRAS="api" or "worker" or "api,worker"
ARG SERVICE_EXTRAS="api"

# Install base deps + extras into system site-packages
# Base deps:
RUN uv pip install --system -r pyproject.toml
RUN uv pip install --system -r pyproject.toml --extra api || echo "no api extra; skipping"
RUN rm -rf src/worker
EXPOSE 8000
ENV PYTHONPATH=/app/src:/app
ENTRYPOINT ["python", "api_main.py"]

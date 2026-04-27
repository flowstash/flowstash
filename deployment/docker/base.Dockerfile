# Base Dockerfile for Flowstash 
# This image contains the environment and pre-installed local packages

FROM python:3.13-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies from demo/pyproject.toml
# These are the shared dependencies for the demo application
RUN pip install --upgrade pip uv && \
    uv pip install --system \
    "pyyaml>=6.0" \
    "python-dotenv>=1.0.1" \
    "fastapi>=0.100.0" \
    "uvicorn>=0.23.0" \
    "dramatiq[redis]>=1.14.0" \
    "setuptools<70.0.0"

# Copy and install framework packages
COPY packages/flowstash_lib /app/packages/flowstash_lib
COPY packages/flowstash_runtime /app/packages/flowstash_runtime
COPY packages/flowstash_clients /app/packages/flowstash_clients

# Install them as local packages together so uv can resolve internal dependencies
RUN uv pip install --system \
    /app/packages/flowstash_clients \
    /app/packages/flowstash_lib \
    /app/packages/flowstash_runtime

# Default command (can be overridden)
CMD ["python3"]

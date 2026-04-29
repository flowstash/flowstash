import os
import asyncio
import logging
from pathlib import Path
from flowstash.config.env_loader import load_config_dir
from flowstash.runtime import initialize_runtime, run_worker

logging.basicConfig(level=logging.INFO)

# Load config
env = os.getenv("ENVIRONMENT", "dev")
config = load_config_dir("config", environment=env)

# Initialize runtime with auto-import of the tasks directory
runtime = initialize_runtime(
    config,
    auto_import=[
        Path(__file__).parent / "src" / "shared" / "tasks",
        Path(__file__).parent / "src" / "worker" / "tasks",
    ],
)
if __name__ == "__main__":
    # If run directly, start the worker
    print(f"Worker starting with backend: {config.backend}")
    asyncio.run(run_worker(config))

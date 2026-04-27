"""
Worker entrypoint utilities.

Provides a minimal entrypoint for worker process configuration.
"""
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.runtime.wiring.runtime import build_worker_runtime, Runtime


def configure_worker_process(config: RuntimeConfig) -> Runtime:
    """
    Minimal entrypoint for worker process.
    
    Usage in worker service:
        from flowstash.runtime.worker.entrypoint import configure_worker_process
        from flowstash.config.loader import load_runtime_config
        
        config = load_runtime_config("/path/to/config")
        runtime = configure_worker_process(config)
        
        # Then run dramatiq worker:
        # dramatiq my_tasks_module
    
    Args:
        config: RuntimeConfig with worker and redis configuration
        
    Returns:
        Runtime instance with configured broker
    """
    return build_worker_runtime(config)

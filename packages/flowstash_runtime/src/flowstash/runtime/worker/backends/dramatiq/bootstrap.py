"""
Worker bootstrap utilities.

Provides functions to configure the Dramatiq broker with framework middleware.
"""
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from flowstash.config.runtime_config import RuntimeConfig, BackendType
from flowstash.queue.backend import set_backend
from .dramatiq_backend import DramatiqBackend, FrameworkContextMiddleware


def _build_redis_url(config: RuntimeConfig) -> str:
    """Build Redis URL from config or environment."""
    if os.getenv("REDIS_URL"):
        return os.getenv("REDIS_URL")
        
    if config.backend.dramatiq and config.backend.dramatiq.redis_url:
        return config.backend.dramatiq.redis_url
        
    return "redis://localhost:6379/0"


def configure_worker(config: RuntimeConfig):
    """
    Configure Dramatiq broker with context middleware.
    
    This sets up the broker and middleware for worker processes.
    Note: This is a lower-level function. For most cases, use
    `configure_worker_process()` from entrypoint.py instead.
    
    Args:
        config: RuntimeConfig with worker and redis configuration
        
    Returns:
        The configured broker instance
        
    Raises:
        ValueError: If backend type is not DRAMATIQ
    """
    if config.backend.type != BackendType.DRAMATIQ:
        raise ValueError(f"Unsupported backend for worker: {config.backend.type}")
    
    redis_url = _build_redis_url(config)
    broker = RedisBroker(url=redis_url)
    broker.add_middleware(FrameworkContextMiddleware())
    dramatiq.set_broker(broker)
    set_backend(DramatiqBackend())
    
    # Dramatiq's built-in middlewares (like Prometheus) expect the process_boot 
    # signal to be emitted to initialize their internal states (like prometheus Counters).
    # Since we initialize the Worker programmatically (not via dramatiq CLI),
    # we must emit this manually. We emit it here so that producers (API) also 
    # initialize the metrics, preventing crashes during enqueue.
    broker.emit_after("process_boot")
    
    return broker

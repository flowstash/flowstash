import logging
import asyncio
import sys
from flowstash.config.runtime_config import RuntimeConfig, BackendType
from flowstash.queue.consumer import TaskConsumer

logger = logging.getLogger(__name__)


def build_worker_consumer(config: RuntimeConfig) -> TaskConsumer:
    """
    Build the appropriate TaskConsumer for the given configuration.
    """
    if config.backend.type == BackendType.DRAMATIQ:
        from .backends.dramatiq.dramatiq_consumer import DramatiqConsumer

        return DramatiqConsumer(config)
    elif config.backend.type == BackendType.MANAGED or (
        len(sys.argv) > 1 and sys.argv[1] == "run-task"
    ):
        from .backends.managed.managed_consumer import ManagedConsumer

        return ManagedConsumer(config)
    elif config.backend.type == BackendType.ASYNC:
        raise ValueError(
            "BackendType.ASYNC is for in-process execution and does not support "
            "a standalone worker process. "
            "Launch the API server instead."
        )
    else:
        raise ValueError(f"Unsupported backend: {config.backend.type}")


async def run_worker(config: RuntimeConfig):
    """
    Unified entry point to start a worker process.
    """
    logger.info(f"Starting worker with backend: {config.backend.type}")

    if config.backend.type == BackendType.MANAGED:
        from .backends.managed.main import run_managed_http_server
        from .backends.managed.managed_consumer import is_managed_job_mode

        if is_managed_job_mode():
            consumer = build_worker_consumer(config)
        else:
            await run_managed_http_server(config)
            return
    else:
        consumer = build_worker_consumer(config)

    # Print registered tasks and consumers
    try:
        from flowstash.queue.backend import get_backend
        from flowstash.pipelines.consumer import get_registered_consumers

        logger.info("--- Registered Worker Endpoints ---")

        if config.backend.type == BackendType.DRAMATIQ:
            import dramatiq

            backend = get_backend()
            actors = dramatiq.get_broker().get_declared_actors()
            scheduled_jobs = backend.get_scheduled_jobs()
            sched_map = {job.scheduled_job_id: job.schedule for job in scheduled_jobs}

            job_count = len(list(actors))
            logger.info(f"Registered Tasks: ({job_count})")
            for actor in actors:
                underlying_fn = getattr(actor, "fn", getattr(actor, "__call__", actor))
                fn_name = getattr(
                    actor, "actor_name", getattr(underlying_fn, "__name__", str(actor))
                )
                fn_mod = getattr(underlying_fn, "__module__", "")
                full_id = f"{fn_mod}.{getattr(underlying_fn, '__name__', fn_name)}"

                # Fetch schedule if exists
                schedule = sched_map.get(full_id)
                if not schedule:
                    for sid, sval in sched_map.items():
                        if fn_name in sid or sid.replace("scheduled:", "") in full_id:
                            schedule = sval
                            break

                sched_str = f" [Schedule: {schedule}]" if schedule else ""
                logger.info(f" - {fn_name}{sched_str}")

        feed_consumers = get_registered_consumers()
        if feed_consumers:
            logger.info(f"Registered Feed Consumers: ({len(feed_consumers)})")
            for fc in feed_consumers:
                batch_info = (
                    f" (Batching enabled, max: {fc.max_batch_size})" if fc.batch else ""
                )
                logger.info(
                    f" - Feed: {fc.feed_id} -> {fc.subscription_name}{batch_info}"
                )

        logger.info("-----------------------------------")
    except Exception as e:
        logger.warning(f"Could not print tasks/consumers on startup: {e}")

    await consumer.start()

    if config.backend.type == BackendType.MANAGED:
        return

    # If the consumer didn't block (like Dramatiq), we wait here
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Worker runner cancelled, stopping consumer...")
        await consumer.stop()
    except KeyboardInterrupt:
        logger.info("Interrupted, stopping consumer...")
        await consumer.stop()

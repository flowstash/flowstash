import dramatiq
from dramatiq.worker import Worker
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.queue.consumer import TaskConsumer
from flowstash.runtime.wiring.runtime import initialize_runtime
from .dramatiq_backend import FrameworkContextMiddleware
from .consumer_middleware import ConsumerMiddleware

class DramatiqConsumer(TaskConsumer):
    """
    TaskConsumer implementation for Dramatiq.
    
    Configures the broker with execution middleware and starts the worker.
    """
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.worker: Worker = None
        self.scheduler = None

    async def start(self) -> None:
        # 1. Initialize runtime if not already initialized
        from flowstash.queue.backend import _backend
        if _backend is None:
            initialize_runtime(self.config)
        # 2. Add worker-specific middleware
        broker = dramatiq.get_broker()
        broker.add_middleware(FrameworkContextMiddleware())
        broker.add_middleware(ConsumerMiddleware())
        
        # 3. Start the worker
        self.worker = Worker(broker)
        self.worker.start()
        
        # 4. Start the APScheduler if we have scheduled jobs
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
            from flowstash.queue.backend import _backend
            import logging
            
            logger = logging.getLogger(__name__)
            
            if hasattr(_backend, '_scheduled_jobs') and _backend._scheduled_jobs:
                self.scheduler = BackgroundScheduler()
                
                for job_info in _backend._scheduled_jobs:
                    scheduled_job_id = job_info['scheduled_job_id']
                    
                    def enqueue_job(func=job_info['func'], args=job_info['args'], kwargs=job_info['kwargs'], 
                                    tags=job_info['tags'], job_id=scheduled_job_id):
                        headers = _backend._prepare_headers(None, tags, scheduled_job_id=job_id)
                        
                        actor = getattr(func, '_backend_handler', func)
                        if hasattr(actor, 'send_with_options'):
                            actor.send_with_options(args=args, kwargs=kwargs, headers=headers)
                            logger.info(f"Enqueued scheduled job {job_id}")
                        else:
                            logger.error(f"Cannot enqueue {job_id}: is not a Dramatiq actor")
                            
                    self.scheduler.add_job(
                        enqueue_job,
                        CronTrigger.from_crontab(job_info['schedule'].cron),
                        id=scheduled_job_id,
                        replace_existing=True,
                        name=job_info['actor_name']
                    )
                
                self.scheduler.start()
        except ImportError as e:
            import logging
            logging.getLogger(__name__).warning(f"Could not start APScheduler: {e}")
        
    async def stop(self) -> None:
        if self.worker:
            self.worker.stop()
        if self.scheduler:
            self.scheduler.shutdown()

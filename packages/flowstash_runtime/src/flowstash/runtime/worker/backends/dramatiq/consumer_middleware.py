import threading
import asyncio
import logging
import dramatiq
from flowstash.pipelines.consumer import ConsumerRunner

logger = logging.getLogger(__name__)

class ConsumerMiddleware(dramatiq.Middleware):
    def __init__(self):
        self._loop: asyncio.AbstractEventLoop = None
        self._thread: threading.Thread = None
        self._runner: ConsumerRunner = None
        self._stop_event = threading.Event()

    def after_worker_boot(self, broker, worker):
        """Called when the worker process starts."""
        logger.info("Starting ConsumerRunner thread...")
        
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ConsumerRunnerThread")
        self._thread.start()

    def before_worker_shutdown(self, broker, worker):
        """Called when the worker process is shutting down."""
        logger.info("Stopping ConsumerRunner thread...")
        if self._loop:
            asyncio.run_coroutine_threadsafe(self._runner.stop(), self._loop)
            # Give it a moment to stop
            self._thread.join(timeout=5.0)

    def _run_loop(self):
        """Runs the asyncio loop in a separate thread."""
        logger.info("ConsumerRunner loop started.")
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        self._runner = ConsumerRunner()
        
        self._loop.run_until_complete(self._runner.start())
        
        # Keep loop running until stopped
        try:
            self._loop.run_forever()
        except Exception as e:
            logger.error(f"ConsumerRunner loop error: {e}")
        finally:
            self._loop.close()
            logger.info("ConsumerRunner loop closed.")

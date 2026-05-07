import asyncio
import logging
from datetime import UTC, datetime
from typing import Optional

logger = logging.getLogger(__name__)


class ManagedTaskDrainController:
    def __init__(self) -> None:
        self.active_requests = 0
        self._draining = False
        self._lock = asyncio.Lock()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._shutdown_started_at: Optional[datetime] = None

    async def begin_request(self, kind: str, task_ref: str) -> bool:
        async with self._lock:
            if self._draining:
                logger.info(
                    "Rejecting managed %s while draining: %s",
                    kind,
                    task_ref,
                )
                return False

            self.active_requests += 1
            self._idle_event.clear()
            logger.info(
                "Accepted managed %s: %s (active=%s)",
                kind,
                task_ref,
                self.active_requests,
            )
            return True

    async def finish_request(self, kind: str, task_ref: str) -> None:
        async with self._lock:
            if self.active_requests > 0:
                self.active_requests -= 1
            if self.active_requests == 0:
                self._idle_event.set()
            logger.info(
                "Finished managed %s: %s (active=%s)",
                kind,
                task_ref,
                self.active_requests,
            )

    async def start_draining(self, reason: str) -> None:
        async with self._lock:
            if self._draining:
                return
            self._draining = True
            self._shutdown_started_at = datetime.now(UTC)
            logger.info("Managed worker entered draining mode: %s", reason)
            if self.active_requests == 0:
                self._idle_event.set()

    async def wait_for_idle(self, timeout_s: float) -> bool:
        try:
            await asyncio.wait_for(self._idle_event.wait(), timeout=timeout_s)
            return True
        except TimeoutError:
            logger.warning(
                "Managed worker drain timed out with %s active request(s)",
                self.active_requests,
            )
            return False

    def is_draining(self) -> bool:
        return self._draining

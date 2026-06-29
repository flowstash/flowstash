"""
Worker-side client for the lease broker.

One process-singleton WebSocket connection multiplexes the acquire/release of
every task this worker process runs. The connection itself is the liveness
signal — if it drops, the broker frees this worker's leases after a grace window
unless we reconnect and re-assert them. We track our own held run_ids locally and
re-assert the whole set on every (re)connect.

Safe rollout: if the broker is not configured (no URL/token), or the optional
``websockets`` dependency is missing, the client is *disabled* and callers run
without the guard (no behaviour change). Once enabled, an unreachable broker
makes ``acquire`` return ``UNAVAILABLE`` so the caller fails closed (HTTP 503 /
job no-op) rather than risk a duplicate.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

try:  # optional dependency — absence simply disables the guard
    import websockets
except Exception:  # pragma: no cover - import guard
    websockets = None  # type: ignore

logger = logging.getLogger(__name__)

# acquire() outcomes
ACQUIRED = "ACQUIRED"
BUSY = "BUSY"
COMPLETED = "COMPLETED"
UNAVAILABLE = "UNAVAILABLE"

# release() statuses
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"


class LeaseBusy(Exception):
    """Raised when the run's lease is held elsewhere (or the broker is
    unreachable) — the caller should respond with a retryable 503 so Cloud Tasks
    redelivers later, by which point the holder has finished or its lease expired."""

    def __init__(self, run_id: str):
        super().__init__(f"lease busy for run_id={run_id}")
        self.run_id = run_id


@dataclass
class AcquireResult:
    outcome: str  # ACQUIRED | BUSY | COMPLETED | UNAVAILABLE

    @property
    def acquired(self) -> bool:
        return self.outcome == ACQUIRED

    @property
    def duplicate(self) -> bool:
        return self.outcome == COMPLETED

    @property
    def busy(self) -> bool:
        return self.outcome in (BUSY, UNAVAILABLE)


class LeaseBrokerClient:
    def __init__(
        self,
        url: str,
        token: str,
        *,
        acquire_timeout: float = 10.0,
        reconnect_min: float = 0.5,
        reconnect_max: float = 10.0,
    ) -> None:
        self._url = url
        self._token = token
        self._acquire_timeout = acquire_timeout
        self._reconnect_min = reconnect_min
        self._reconnect_max = reconnect_max
        # Stable per-process identity so a reconnecting worker reclaims (transfers)
        # its own leases instead of conflicting with its not-yet-reaped old conn.
        self._worker_id = os.getenv("CLOUD_RUN_EXECUTION") or uuid.uuid4().hex

        self._held: Set[str] = set()
        self._waiters: Dict[str, List[asyncio.Future]] = {}
        self._ws = None
        self._connected = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._run_task: Optional[asyncio.Task] = None
        self._closing = False

    # ── lifecycle ────────────────────────────────────────────────────────

    def ensure_started(self) -> None:
        """Start the background connect loop once, on the current event loop."""
        if self._run_task is None or self._run_task.done():
            self._run_task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        self._closing = True
        if self._run_task is not None:
            self._run_task.cancel()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    def _connect_url(self) -> str:
        sep = "&" if "?" in self._url else "?"
        return f"{self._url}{sep}worker_id={self._worker_id}"

    async def _run(self) -> None:
        backoff = self._reconnect_min
        headers = {"Authorization": f"Bearer {self._token}"}
        while not self._closing:
            try:
                async with websockets.connect(
                    self._connect_url(), additional_headers=headers, open_timeout=10
                ) as ws:
                    self._ws = ws
                    backoff = self._reconnect_min
                    if self._held:
                        # Re-assert everything we still believe we own.
                        await self._send({"op": "reassert", "run_ids": list(self._held)})
                    self._connected.set()
                    async for raw in ws:
                        try:
                            self._on_message(json.loads(raw))
                        except Exception:
                            logger.exception("lease client: bad message %r", raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("lease broker connection lost: %s", e)
            finally:
                self._connected.clear()
                self._ws = None
            if self._closing:
                break
            # Jittered backoff so a broker restart doesn't trigger a synchronized
            # reconnect storm from every worker at once.
            await asyncio.sleep(backoff * (0.5 + 0.5 * random.random()))
            backoff = min(backoff * 2, self._reconnect_max)

    # ── messaging ──────────────────────────────────────────────────────────

    def _on_message(self, msg: dict) -> None:
        op = msg.get("op")
        if op in ("granted", "refused"):
            run_id = msg.get("run_id")
            outcome = ACQUIRED if op == "granted" else msg.get("reason", BUSY)
            self._resolve(run_id, outcome)
        elif op == "revoked":
            # Another worker took over (split-brain after a long stall). We can't
            # force-cancel in-flight work; drop ownership and rely on step
            # idempotency. Logged loudly so it's visible.
            run_id = msg.get("run_id")
            self._held.discard(run_id)
            logger.error("lease REVOKED run_id=%s reason=%s", run_id, msg.get("reason"))
        elif op == "ping":
            asyncio.ensure_future(self._send_safe({"op": "pong"}))

    def _resolve(self, run_id: Optional[str], outcome: str) -> None:
        for fut in self._waiters.pop(run_id, []):
            if not fut.done():
                fut.set_result(outcome)

    async def _send(self, msg: dict) -> None:
        async with self._send_lock:
            if self._ws is None:
                raise ConnectionError("lease broker not connected")
            await self._ws.send(json.dumps(msg))

    async def _send_safe(self, msg: dict) -> None:
        try:
            await self._send(msg)
        except Exception:
            pass

    # ── public API ──────────────────────────────────────────────────────────

    async def acquire(
        self, run_id: str, entry_point: Optional[str] = None, timeout: Optional[float] = None
    ) -> AcquireResult:
        self.ensure_started()
        timeout = timeout or self._acquire_timeout
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return AcquireResult(UNAVAILABLE)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._waiters.setdefault(run_id, []).append(fut)
        try:
            await self._send({"op": "acquire", "run_id": run_id, "entry_point": entry_point})
        except Exception:
            self._discard_waiter(run_id, fut)
            return AcquireResult(UNAVAILABLE)

        try:
            outcome = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._discard_waiter(run_id, fut)
            return AcquireResult(UNAVAILABLE)

        if outcome == ACQUIRED:
            self._held.add(run_id)
        return AcquireResult(outcome)

    async def release(self, run_id: str, status: str = SUCCEEDED) -> None:
        self._held.discard(run_id)
        # Best-effort: if the send fails, the broker frees the lease via the
        # grace window when the connection drops anyway.
        await self._send_safe({"op": "release", "run_id": run_id, "status": status})

    def _discard_waiter(self, run_id: str, fut: asyncio.Future) -> None:
        waiters = self._waiters.get(run_id)
        if waiters and fut in waiters:
            waiters.remove(fut)
            if not waiters:
                self._waiters.pop(run_id, None)


# ── singleton wiring ─────────────────────────────────────────────────────────

_client: Optional[LeaseBrokerClient] = None
_resolved = False


def _derive_broker_url() -> Optional[str]:
    explicit = os.getenv("LEASE_BROKER_URL")
    if explicit:
        return explicit
    api = os.getenv("FLOWSTASH_API_URL") or os.getenv("MANAGED_API_URL")
    if not api:
        return None
    api = api.rstrip("/")
    if api.startswith("https://"):
        return "wss://" + api[len("https://") :] + "/ws/leases"
    if api.startswith("http://"):
        return "ws://" + api[len("http://") :] + "/ws/leases"
    return None


def get_lease_client() -> Optional[LeaseBrokerClient]:
    """Return the process-singleton lease client, or None when the guard is
    disabled (broker not configured, explicitly turned off, or ``websockets``
    not installed). Callers treat None as 'run without the guard'."""
    global _client, _resolved
    if _resolved:
        return _client
    _resolved = True

    # Default OFF: deploying the worker code is inert until ops explicitly enables
    # the guard (after the broker is deployed and validated). Otherwise an
    # unreachable broker would fail every task closed (503).
    if os.getenv("LEASE_BROKER_ENABLED", "false").lower() not in ("1", "true", "yes"):
        logger.info("lease broker disabled (set LEASE_BROKER_ENABLED=true to enable)")
        return None
    if websockets is None:
        logger.warning("lease broker disabled: 'websockets' not installed")
        return None
    url = _derive_broker_url()
    token = os.getenv("MANAGED_AUTH_TOKEN", "")
    if not url or not token:
        logger.info("lease broker disabled: URL/token not configured")
        return None

    _client = LeaseBrokerClient(url, token)
    logger.info("lease broker enabled: %s", url)
    return _client

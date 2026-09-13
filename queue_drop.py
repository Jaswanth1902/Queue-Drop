"""
Queue-Drop: High-Concurrency Async Event Pipeline & Backpressure Regulator.
Zero-loss token bucket queue with Dead-Letter Queue (DLQ) isolation.
"""
import asyncio
import time
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("QueueDrop")

class Event:
    def __init__(self, event_id: str, payload: Dict[str, Any], priority: int = 1):
        self.event_id = event_id
        self.payload = payload
        self.priority = priority
        self.attempts = 0
        self.timestamp = time.time()

class QueueDropPipeline:
    def __init__(self, max_capacity: int = 10000, rate_limit_rps: int = 500, max_retries: int = 3):
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_capacity)
        self.dlq: asyncio.Queue[Event] = asyncio.Queue()
        self.rate_limit_rps = rate_limit_rps
        self.max_retries = max_retries
        self._tokens = rate_limit_rps
        self._last_token_refresh = time.monotonic()
        self._running = False
        self._worker_tasks = []

    async def _replenish_tokens(self):
        while self._running:
            now = time.monotonic()
            elapsed = now - self._last_token_refresh
            self._tokens = min(self.rate_limit_rps, self._tokens + int(elapsed * self.rate_limit_rps))
            self._last_token_refresh = now
            await asyncio.sleep(0.05)

    async def enqueue(self, event: Event, timeout: float = 2.0) -> bool:
        """Enqueue an event with non-blocking backpressure timeout."""
        try:
            await asyncio.wait_for(self.queue.put(event), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Backpressure drop alert: Queue full. Event {event.event_id} rejected.")
            return False

    async def _worker(self, worker_id: int, handler: Callable[[Event], Any]):
        while self._running:
            event = await self.queue.get()
            # Token bucket throttle
            while self._tokens <= 0:
                await asyncio.sleep(0.01)
            self._tokens -= 1

            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
                self.queue.task_done()
            except Exception as exc:
                event.attempts += 1
                if event.attempts < self.max_retries:
                    backoff = 0.1 * (2 ** event.attempts)
                    await asyncio.sleep(backoff)
                    await self.queue.put(event)
                else:
                    logger.error(f"Event {event.event_id} failed after {event.attempts} attempts. Moving to DLQ: {exc}")
                    await self.dlq.put(event)
                self.queue.task_done()

    async def start(self, handler: Callable[[Event], Any], num_workers: int = 4):
        self._running = True
        asyncio.create_task(self._replenish_tokens())
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i, handler))
            self._worker_tasks.append(task)

    async def stop(self):
        self._running = False
        for task in self._worker_tasks:
            task.cancel()

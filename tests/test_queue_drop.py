import asyncio
import pytest
from queue_drop import QueueDropPipeline, Event

@pytest.mark.asyncio
async def test_queue_drop_throughput():
    processed = []
    async def handler(e: Event):
        processed.append(e.event_id)

    pipeline = QueueDropPipeline(max_capacity=100, rate_limit_rps=200)
    await pipeline.start(handler, num_workers=2)

    for i in range(10):
        ok = await pipeline.enqueue(Event(f"evt_{i}", {"val": i}))
        assert ok is True

    await asyncio.sleep(0.2)
    assert len(processed) == 10
    await pipeline.stop()

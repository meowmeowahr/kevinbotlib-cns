import asyncio
import pytest
import time
from kevinbotlib_cns.server.cns_server import CNSServer
from kevinbotlib_cns.client.async_client import CNSAsyncClient


@pytest.fixture(scope="module")
def cns_server():
    server = CNSServer()
    server.start(port=4801)  # Use a different port to avoid conflicts
    # Give it a moment to start
    time.sleep(1)
    yield server
    server.stop()


@pytest.mark.asyncio
async def test_client_operations(cns_server):
    client = CNSAsyncClient("ws://localhost:4801/cns", "pytest-client")

    await client.connect()
    try:
        # Test Set
        await client.set("test/pytest", {"foo": "bar"})

        # Test Get
        data = await client.get("test/pytest")
        assert data == {"foo": "bar"}

        # Test Set Int
        await client.set("test/pytest", 123)

        # Test Get Int
        data = await client.get("test/pytest")
        assert data == 123

        # Test Get Null
        data = await client.get("test/nonexistent")
        assert data is None

        # Test Topic Count
        count = await client.topic_count()
        assert count >= 1

        # Test Topics
        topics = await client.get_topics()
        assert "test/pytest" in topics

        # Test Subscribe
        received_updates = []

        def callback(topic, payload):
            received_updates.append((topic, payload))

        await client.subscribe("test/pytest", callback)

        # Trigger update
        await client.set("test/pytest", {"foo": "baz"})

        # Wait for callback
        for _ in range(10):
            if received_updates:
                break
            await asyncio.sleep(0.1)

        assert len(received_updates) > 0
        assert received_updates[-1][1] == {"foo": "baz"}

    finally:
        await client.disconnect()

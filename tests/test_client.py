import pytest
import time
import threading
from kevinbotlib_cns.server.cns_server import CNSServer
from kevinbotlib_cns.client.client import CNSClient

@pytest.fixture(scope="module")
def cns_server():
    server = CNSServer()
    server.start(port=4802)
    time.sleep(1)
    yield server
    server.stop()

def test_sync_client(cns_server):
    client = CNSClient("ws://localhost:4802/cns", "sync-client")
    
    client.connect()
    try:
        # Test Set
        client.set("test/sync", {"foo": "bar"})
        
        # Test Get
        data = client.get("test/sync")
        assert data == {"foo": "bar"}
        
        # Test Topic Count
        count = client.topic_count()
        assert count >= 1
        
        # Test Topics
        topics = client.topics()
        assert "test/sync" in topics
        
        # Test Subscribe
        received_updates = []
        event = threading.Event()
        
        def callback(topic, payload):
            received_updates.append((topic, payload))
            event.set()
            
        client.subscribe("test/sync", callback)
        
        # Trigger update
        client.set("test/sync", {"foo": "baz"})
        
        # Wait for callback
        assert event.wait(timeout=2.0)
        assert len(received_updates) > 0
        assert received_updates[-1][1] == {"foo": "baz"}
        
    finally:
        client.disconnect()

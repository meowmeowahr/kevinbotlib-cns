import asyncio
import threading
from typing import Callable, Optional

from kevinbotlib_cns.client.async_client import CNSAsyncClient
from kevinbotlib_cns.types import JSONType


class CNSClient:
    """
    Synchronous client for the KevinbotLib CNS Server.
    """

    def __init__(self, url: str, client_id: str):
        self.url = url
        self.client_id = client_id
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._async_client: Optional[CNSAsyncClient] = None
        self._connected = threading.Event()

        self._async_client = CNSAsyncClient(self.url, self.client_id)
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()

    def connect(self):
        """
        Attempt to connect to the KevinbotLib CNS server.
        :return:
        """
        if not self._async_client:
            raise ConnectionError("Failed to create async client")
        
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_client.connect(), self._loop)
            future.result(timeout=0.1)  # Add timeout to prevent indefinite blocking
        except Exception as e:
            # Connection failed, but don't leave things in a bad state
            raise ConnectionError(f"Failed to connect to CNS: {e}") from e

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def disconnect(self):
        """
        Disconnect from the CNS server.
        :return:
        """
        if not self._loop:
            return

        if self._async_client:
            try:
                future = asyncio.run_coroutine_threadsafe(self._async_client.disconnect(), self._loop)
                future.result(timeout=2.0)
            except Exception:
                pass  # Ignore errors during disconnect
        

    def flushdb(self) -> str:
        """
        Wipe all topics in the CNS database.

        **WARNING**: This is a destructive operation.

        :return: Server-reported timestamp
        """
        if not self._async_client or not self._loop:
            raise ConnectionError("Not connected to CNS server")
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.flushdb(), self._loop
        )
        return future.result()

    def ping(self) -> float | None:
        """
        Ping the CNS server.
        :return: Seconds of (round-trip) latency
        """
        if not self._async_client or not self._loop:
            return None
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.ping(), self._loop
        )
        return future.result(timeout=2)

    def delete(self, topic: str) -> str:
        """
        Delete a CNS topic from the database.
        :param topic: Topic to delete.
        :return: Server-reported timestamp
        """
        if not self._async_client or not self._loop:
            raise ConnectionError("Not connected to CNS server")
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.delete(topic), self._loop
        )
        return future.result(timeout=2)

    def set(self, topic: str, data: JSONType) -> str:
        """
        Set the value of a CNS topic.
        :param topic: Topic to set.
        :param data: Data to set.
        :return: Server-reported timestamp
        """
        if not self._async_client or not self._loop:
            raise ConnectionError("Not connected to CNS server")
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.set(topic, data), self._loop
        )
        return future.result(timeout=2)

    def get(self, topic: str) -> JSONType | None:
        """
        Get the value of a CNS topic.
        :param topic: Topic to get.
        :return: JSON-serializable data. Returns None if topic is not found, or if the data is null.
        """
        if not self._async_client or not self._loop:
            return None
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.get(topic), self._loop
        )
        return future.result(timeout=2)

    def subscribe(self, topic: str, callback: Callable[[str, JSONType], None]) -> None:
        """
        Subscribe to a CNS topic.

        **NOTE**: The callback will be called from a background thread. The callback must not block.

        :param topic: Topic to subscribe to.
        :param callback: Callback to execute when a topic changes.
        :return:
        """
        if not self._async_client or not self._loop:
            raise ConnectionError("Not connected to CNS server")
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.subscribe(topic, callback), self._loop
        )
        future.result(timeout=2)

    def topic_count(self) -> int:
        """
        Get the number of topics on the CNS Server's database.
        :return: Topic count
        """
        if not self._async_client or not self._loop:
            return 0
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.topic_count(), self._loop
        )
        return future.result()

    def topics(self) -> list[str]:
        """
        Get a list of topics in the CNS Server's database.
        :return: List of topics.
        """
        if not self._async_client or not self._loop:
            return []
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.get_topics(), self._loop
        )
        return future.result()

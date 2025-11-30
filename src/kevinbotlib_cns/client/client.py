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

    def connect(self):
        """
        Attempt to connect to the KevinbotLib CNS server.
        :return:
        """
        if self._thread and self._thread.is_alive():
            return

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        future = asyncio.run_coroutine_threadsafe(self._connect_async(), self._loop)
        future.result()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._async_client = CNSAsyncClient(self.url, self.client_id)
        self._loop.run_forever()

    async def _connect_async(self):
        await self._async_client.connect()

    def disconnect(self):
        """
        Disconnect from the CNS server.
        :return:
        """
        if not self._loop:
            return

        future = asyncio.run_coroutine_threadsafe(self._async_client.disconnect(), self._loop)
        future.result()
        
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop = None
        self._thread = None
        self._async_client = None

    def flushdb(self) -> str:
        """
        Wipe all topics in the CNS database.

        **WARNING**: This is a destructive operation.

        :return: Server-reported timestamp
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.flushdb(), self._loop
        )
        return future.result()

    def ping(self) -> float | None:
        """
        Ping the CNS server.
        :return: Seconds of (round-trip) latency
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.ping(), self._loop
        )
        return future.result()

    def delete(self, topic: str) -> str:
        """
        Delete a CNS topic from the database.
        :param topic: Topic to delete.
        :return: Server-reported timestamp
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.delete(topic), self._loop
        )
        return future.result()

    def set(self, topic: str, data: JSONType) -> str:
        """
        Set the value of a CNS topic.
        :param topic: Topic to set.
        :param data: Data to set.
        :return: Server-reported timestamp
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.set(topic, data), self._loop
        )
        return future.result()

    def get(self, topic: str) -> JSONType | None:
        """
        Get the value of a CNS topic.
        :param topic: Topic to get.
        :return: JSON-serializable data. Returns None if topic is not found, or if the data is null.
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.get(topic), self._loop
        )
        return future.result()

    def subscribe(self, topic: str, callback: Callable[[str, JSONType], None]) -> None:
        """
        Subscribe to a CNS topic.

        **NOTE**: The callback will be called from a background thread. The callback must not block.

        :param topic: Topic to subscribe to.
        :param callback: Callback to execute when a topic changes.
        :return:
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.subscribe(topic, callback), self._loop
        )
        future.result()

    def topic_count(self) -> int:
        """
        Get the number of topics on the CNS Server's database.
        :return: Topic count
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.topic_count(), self._loop
        )
        return future.result()

    def topics(self) -> list[str]:
        """
        Get a list of topics in the CNS Server's database.
        :return: List of topics.
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.get_topics(), self._loop
        )
        return future.result()

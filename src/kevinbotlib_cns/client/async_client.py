import asyncio
import json
import time
import traceback
from typing import Any, Awaitable, Callable, Optional

import orjson
import websockets
from loguru import logger

from kevinbotlib_cns.types import JSONType


class CNSAsyncClient:
    """
    Asyncio Client for the KevinbotLib CNS Server
    """

    def __init__(self, url: str, client_id: str, timeout: float = 2.0):
        self.url = url
        self.client_id = client_id
        self.websocket: Optional[websockets.ClientConnection] = None
        self.timeout = timeout
        self._listen_task: Optional[asyncio.Task] = None
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._subscriptions: dict[
            str, list[Callable[[str, JSONType], Awaitable[None] | None]]
        ] = {}
        self._connected = False

    async def connect(self):
        """
        Connect to the KevinbotLib CNS Server
        :return:
        """

        logger.info(f"Connecting to CNS at {self.url} with ID {self.client_id}")

        # Clean up old connection if reconnecting
        self._connected = False

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self.websocket:
            await self.websocket.close()

        # Clear any pending requests from previous connection
        for key, future in list(self._pending_requests.items()):
            if not future.done():
                future.set_exception(ConnectionError("Connection reset"))
        self._pending_requests.clear()

        # Note: subscriptions are preserved across reconnects

        self.websocket = await websockets.connect(
            self.url,
            additional_headers={"Client-ID": self.client_id},
            max_size=50 * 1024 * 1024,
            open_timeout=self.timeout,
            ping_timeout=self.timeout,
        )
        self._connected = True
        self._listen_task = asyncio.create_task(self._listen())
        logger.info("Connected to CNS")

        # Resubscribe to all topics
        for topic in self._subscriptions:
            logger.info(f"Resubscribing to {topic}")
            await self.websocket.send(
                orjson.dumps({"action": "sub", "topic": topic}).decode("utf-8")
            )

    async def disconnect(self):
        """
        Disconnect from the KevinbotLib CNS Server
        :return:
        """

        self._connected = False
        if self.websocket:
            await self.websocket.close()
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        logger.info("Disconnected from CNS")

    async def _listen(self):
        try:
            async for message in self.websocket:
                try:
                    data = orjson.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON: {message}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        except websockets.ConnectionClosed as e:
            logger.info(f"Connection closed {e!r}")
        except Exception as e:
            logger.error(f"Listen loop error: {e}")
        finally:
            self._connected = False
            for key, future in list(self._pending_requests.items()):
                if not future.done():
                    future.set_exception(ConnectionError("Connection closed before response received"))
            self._pending_requests.clear()

    async def _handle_message(self, data: dict[str, Any]):
        action = data.get("action")

        if action == "pub":
            topic = data.get("topic")
            payload = data.get("data")
            if topic in self._subscriptions:
                for callback in self._subscriptions[topic]:
                    try:
                        res = callback(topic, payload)
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception as e:
                        logger.error(f"Error in subscription callback for {topic}: {e}")
            return

        future_key = None
        result = None

        if action == "get":
            topic = data.get("topic")
            future_key = f"get:{topic}"
            result = data.get("data")
        elif action == "set":
            topic = data.get("topic")
            future_key = f"set:{topic}"
            result = data.get("ts")
        elif action == "del":
            topic = data.get("topic")
            future_key = f"del:{topic}"
            result = data.get("ts")
        elif action == "sub":
            topic = data.get("topic")
            future_key = f"sub:{topic}"
            result = data.get("ts")
        elif action == "pong":
            future_key = "pong"
            result = data.get("ts")
        elif action == "tcnt":
            future_key = "tcnt"
            result = data.get("count")
        elif action == "topics":
            future_key = "topics"
            result = data.get("topics")
        elif action == "flush":
            future_key = "flush"
            result = data.get("ts")
        elif "error" in data:
            logger.error(f"CNS Error: {data['error']}")
            return

        if future_key and future_key in self._pending_requests:
            future = self._pending_requests.pop(future_key)
            if not future.done():
                future.set_result(result)

    def _create_future(self, key: str) -> asyncio.Future:
        """Create and register a future for a pending request."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_requests[key] = future
        return future

    async def _wait_for_response(self, key: str) -> Any:
        """Wait for a response that was already registered with _create_future."""
        if key not in self._pending_requests:
            raise RuntimeError(f"No pending request for {key}")
        future = self._pending_requests[key]
        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop(key, None)
            raise TimeoutError(f"Timed out waiting for response to {key}")

    async def ping(self) -> float | None:
        """
        Ping the CNS Server
        :return: Seconds to response
        """
        if not self.websocket or not self._connected:
            logger.error("Couldn't ping, CNS is not connected")
            return None

        time_start = time.monotonic()
        response_future = self._create_future("pong")
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "ping"}).decode("utf-8")
            ), self.timeout)
            await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop("pong", None)
            raise TimeoutError("Ping timeout")
        except Exception:
            self._pending_requests.pop("pong", None)
            raise
        time_end = time.monotonic()
        return time_end - time_start

    async def flushdb(self) -> str | None:
        """
        Wipe all topics on the CNS Server's database.

        **WARNING**: This is a destructive operation.

        :return: Server-reported timestamp. None if not connected.
        """
        if not self.websocket or not self._connected:
            logger.error("Couldn't flush database, CNS is not connected")
            return None

        response_future = self._create_future("flush")
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "flush"}).decode("utf-8")
            ), self.timeout)
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop("flush", None)
            raise TimeoutError("Flush timeout")
        except Exception:
            self._pending_requests.pop("flush", None)
            raise

    async def delete(self, topic: str) -> str | None:
        """
        Delete a topic in the CNs database.
        :param topic: Topic to delete
        :return: Topic that was deleted. None if not connected.
        """
        if not self.websocket or not self._connected:
            logger.error(f"Couldn't delete {topic}, CNS is not connected")
            return None

        key = f"del:{topic}"
        response_future = self._create_future(key)
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "del", "topic": topic}).decode("utf-8")
            ), self.timeout)
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop(key, None)
            raise TimeoutError(f"Delete timeout for topic {topic}")
        except Exception:
            self._pending_requests.pop(key, None)
            raise

    async def set(self, topic: str, data: JSONType, timeout: int | None = None) -> str | None:
        """
        Set the value of a CNS topic.
        :param topic: Topic to set.
        :param data: Data to set.
        :param timeout: Millisecond expiration of the topic.
        :return: Server-reported timestamp. None if not connected.
        """
        if not self.websocket or not self._connected:
            logger.error(f"Couldn't set {topic}, CNS is not connected")
            return None

        key = f"set:{topic}"
        response_future = self._create_future(key)
        try:
            if timeout:
                await asyncio.wait_for(
                    self.websocket.send(
                        orjson.dumps(
                            {"action": "set", "topic": topic, "data": data, "timeout": timeout}
                        ).decode("utf-8")
                    ),
                    self.timeout,
                )
            else:
                await asyncio.wait_for(
                    self.websocket.send(
                        orjson.dumps(
                            {"action": "set", "topic": topic, "data": data}
                        ).decode("utf-8")
                    ),
                    self.timeout,
                )
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop(key, None)
            raise TimeoutError(f"Set timeout for topic {topic}")
        except Exception:
            self._pending_requests.pop(key, None)
            raise

    async def get(self, topic: str) -> JSONType | None:
        """
        Get a value on a specific CNS topic.
        :param topic: CNS topic to get from.
        :return: Data from that topic. None if not connected or if the data is null.
        """
        if not self.websocket or not self._connected:
            logger.error(f"Couldn't get {topic}, CNS is not connected")
            return None

        key = f"get:{topic}"
        response_future = self._create_future(key)
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "get", "topic": topic}).decode("utf-8")
            ), self.timeout)
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop(key, None)
            raise TimeoutError(f"Get timeout for topic {topic}")
        except Exception:
            self._pending_requests.pop(key, None)
            raise

    async def subscribe(
            self, topic: str, callback: Callable[[str, JSONType], Awaitable[None] | None]
    ):
        """
        Subscribe to a CNS topic.
        :param topic: The topic to subscribe to.
        :param callback: Callback to execute when data is received on that topic.
        :return:
        """
        if not self.websocket or not self._connected:
            logger.error(f"Couldn't subscribe to {topic}, CNS is not connected")
            return

        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
            key = f"sub:{topic}"
            response_future = self._create_future(key)
            try:
                await asyncio.wait_for(self.websocket.send(
                    orjson.dumps({"action": "sub", "topic": topic}).decode("utf-8")
                ), self.timeout)
                await asyncio.wait_for(response_future, timeout=5.0)
            except asyncio.TimeoutError:
                self._pending_requests.pop(key, None)
                self._subscriptions.pop(topic, None)
                raise TimeoutError(f"Subscribe timeout for topic {topic}")
            except Exception:
                self._pending_requests.pop(key, None)
                self._subscriptions.pop(topic, None)
                raise

        self._subscriptions[topic].append(callback)

    async def topic_count(self) -> int:
        """
        Get the number of topics in the CNS' database.
        :return: Topic count. 0 if not connected.
        """

        if not self.websocket or not self._connected:
            logger.error("Couldn't get topic count, CNS is not connected")
            return 0

        response_future = self._create_future("tcnt")
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "tcnt"}).decode("utf-8")
            ), self.timeout)
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop("tcnt", None)
            raise TimeoutError("Topic count timeout")
        except Exception:
            self._pending_requests.pop("tcnt", None)
            raise

    async def get_topics(self) -> list[str]:
        """
        Get a list of all topics in the CNS' database.
        :return: List of topics. Empty list if not connected.
        """
        if not self.websocket or not self._connected:
            logger.error("Couldn't get topic list, CNS is not connected")
            return []

        response_future = self._create_future("topics")
        try:
            await asyncio.wait_for(self.websocket.send(
                orjson.dumps({"action": "topics"}).decode("utf-8")
            ), self.timeout)
            return await asyncio.wait_for(response_future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop("topics", None)
            raise TimeoutError("Get topics timeout")
        except Exception:
            self._pending_requests.pop("topics", None)
            raise
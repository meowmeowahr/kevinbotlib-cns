import asyncio
import json
import time
from typing import Any, Awaitable, Callable, Optional, Coroutine

import websockets
from loguru import logger

from kevinbotlib_cns.types import JSONType


class CNSAsyncClient:
    """
    Asyncio Client for the KevinbotLib CNS Server
    """

    def __init__(self, url: str, client_id: str):
        self.url = url
        self.client_id = client_id
        self.websocket: Optional[websockets.ClientConnection] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._subscriptions: dict[
            str, list[Callable[[str, JSONType], Awaitable[None] | None]]
        ] = {}

    async def connect(self):
        """
        Connect to the KevinbotLib CNS Server
        :return:
        """

        logger.info(f"Connecting to CNS at {self.url} with ID {self.client_id}")
        self.websocket = await websockets.connect(
            self.url,
            additional_headers={"Client-ID": self.client_id},
            max_size=50 * 1024 * 1024,
        )
        self._listen_task = asyncio.create_task(self._listen())
        logger.info("Connected to CNS")

    async def disconnect(self):
        """
        Disconnect from the KevinbotLib CNS Server
        :return:
        """

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
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON: {message}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        except websockets.ConnectionClosed as e:
            logger.info(f"Connection closed {e!r}")
        except Exception as e:
            logger.error(f"Listen loop error: {e}")

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
        elif "error" in data:
            logger.error(f"CNS Error: {data['error']}")
            return

        if future_key and future_key in self._pending_requests:
            future = self._pending_requests.pop(future_key)
            if not future.done():
                future.set_result(result)

    async def _wait_for_response(self, key: str) -> Any:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_requests[key] = future
        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            if key in self._pending_requests:
                del self._pending_requests[key]
            raise TimeoutError(f"Timed out waiting for response to {key}")

    async def ping(self) -> float | None:
        """
        Ping the CNS Server
        :return: Seconds to response
        """
        if not self.websocket:
            logger.error(f"Couldn't ping, CNS is not connected")
            return None

        time_start = time.monotonic()
        await self.websocket.send(
            json.dumps({"action": "ping"})
        )
        await self._wait_for_response(f"pong")
        time_end = time.monotonic()
        return time_end - time_start

    async def delete(self, topic: str) -> str | None:
        """
        Delete a topic in the CNs database.
        :param topic: Topic to delete
        :return: Topic that was deleted
        """
        if not self.websocket:
            logger.error(f"Couldn't delete {topic}, CNS is not connected")
            return None

        await self.websocket.send(
            json.dumps({"action": "del", "topic": topic})
        )
        return await self._wait_for_response(f"del:{topic}")

    async def set(self, topic: str, data: JSONType) -> str:
        """
        Set the value of a CNS topic.
        :param topic: Topic to set.
        :param data: Data to set.
        :return: Server-reported timestamp
        """
        if not self.websocket:
            logger.error(f"Couldn't set {topic}, CNS is not connected")
            return None

        await self.websocket.send(
            json.dumps({"action": "set", "topic": topic, "data": data})
        )
        return await self._wait_for_response(f"set:{topic}")

    async def get(self, topic: str) -> JSONType | None:
        """
        Get a value on a specific CNS topic.
        :param topic: CNS topic to get from.
        :return: Data from that topic
        """
        if not self.websocket:
            logger.error(f"Couldn't get {topic}, CNS is not connected")
            return None

        await self.websocket.send(json.dumps({"action": "get", "topic": topic}))
        return await self._wait_for_response(f"get:{topic}")

    async def subscribe(
        self, topic: str, callback: Callable[[str, JSONType], Awaitable[None] | None]
    ):
        """
        Subscribe to a CNS topic.
        :param topic: The topic to subscribe to.
        :param callback: Callback to execute when data is received on that topic.
        :return:
        """
        if not self.websocket:
            raise RuntimeError("Not connected")

        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
            await self.websocket.send(json.dumps({"action": "sub", "topic": topic}))
            await self._wait_for_response(f"sub:{topic}")

        self._subscriptions[topic].append(callback)

    async def topic_count(self) -> int:
        """
        Get the number of topics in the CNS' database.
        :return: Topic count
        """

        if not self.websocket:
            raise RuntimeError("Not connected")

        await self.websocket.send(json.dumps({"action": "tcnt"}))
        return await self._wait_for_response("tcnt")

    async def get_topics(self) -> list[str]:
        """
        Get a list of all topics in the CNS' database.
        :return: List of topics
        """
        if not self.websocket:
            raise RuntimeError("Not connected")

        await self.websocket.send(json.dumps({"action": "topics"}))
        return await self._wait_for_response("topics")

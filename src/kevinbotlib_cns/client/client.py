import orjson
import threading
import time
from typing import Any, Callable, Optional

from loguru import logger
from websocket import create_connection, WebSocket, WebSocketConnectionClosedException

from kevinbotlib_cns.types import JSONType


class CNSClient:
    """
    Synchronous client for the KevinbotLib CNS Server.
    """

    def __init__(self, url: str, client_id: str, timeout: float = 2.0):
        self.url = url
        self.client_id = client_id
        self.timeout = timeout

        self.ws: Optional[WebSocket] = None
        self._connected = False

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self._pending_cond = threading.Condition()
        self._pending_results: dict[str, Any] = {}

        self._subscriptions: dict[str, list[Callable[[str, JSONType], None]]] = {}

    def connect(self):
        """
        Attempt to connect to the KevinbotLib CNS server.
        :return:
        """
        logger.info(f"Connecting to CNS at {self.url} with ID {self.client_id}")

        self.disconnect()

        headers = [f"Client-ID: {self.client_id}"]

        self.ws = create_connection(
            self.url,
            timeout=self.timeout,
            header=headers,
            max_size=50 * 1024 * 1024,
        )

        self._connected = True
        self._stop_event.clear()

        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()

        logger.info("Connected to CNS")

        for topic in self._subscriptions.keys():
            logger.info(f"Resubscribing to {topic}")
            self._send({"action": "sub", "topic": topic})

    def disconnect(self):
        """
        Disconnect from the CNS server.
        :return:
        """
        self._connected = False
        self._stop_event.set()

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._thread = None

        with self._pending_cond:
            for k in list(self._pending_results.keys()):
                self._pending_results[k] = ConnectionError("Disconnected")
            self._pending_cond.notify_all()

        logger.info("Disconnected from CNS")

    def _send(self, obj: dict[str, Any]):
        if not self._connected or not self.ws:
            raise ConnectionError("Not connected")

        msg = orjson.dumps(obj).decode("utf-8")
        self.ws.send(msg)

    def _listen_loop(self):
        try:
            while not self._stop_event.is_set():
                try:
                    raw = self.ws.recv()
                except WebSocketConnectionClosedException:
                    logger.info("WebSocket closed")
                    break
                except Exception as e:
                    logger.error(f"WebSocket read error: {e}")
                    break

                if not raw:
                    continue

                try:
                    data = orjson.loads(raw)
                except Exception:
                    logger.warning(f"Invalid JSON from CNS: {raw}")
                    continue

                self._handle_message(data)

        finally:
            self._connected = False
            with self._pending_cond:
                for k in list(self._pending_results.keys()):
                    self._pending_results[k] = ConnectionError("Connection closed")
                self._pending_cond.notify_all()

    def _handle_message(self, data: dict[str, Any]):
        action = data.get("action")

        if action == "pub":
            topic = data.get("topic")
            payload = data.get("data")
            if topic in self._subscriptions:
                for cb in self._subscriptions[topic]:
                    try:
                        cb(topic, payload)
                    except Exception as e:
                        logger.error(f"Callback error for topic {topic}: {e}")
            return

        future_key = None
        result = None

        if action == "get":
            future_key = f"get:{data.get('topic')}"
            result = data.get("data")
        elif action == "set":
            future_key = f"set:{data.get('topic')}"
            result = data.get("ts")
        elif action == "del":
            future_key = f"del:{data.get('topic')}"
            result = data.get("ts")
        elif action == "sub":
            future_key = f"sub:{data.get('topic')}"
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

        if future_key is not None:
            with self._pending_cond:
                self._pending_results[future_key] = result
                self._pending_cond.notify_all()

    def _wait_for(self, key: str, timeout=5.0):
        deadline = time.monotonic() + timeout

        with self._pending_cond:
            while key not in self._pending_results:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for {key}")
                self._pending_cond.wait(timeout=remaining)

            result = self._pending_results.pop(key)
            if isinstance(result, Exception):
                raise result
            return result

    def ping(self) -> float | None:
        """
        Ping the CNS server.
        :return: Seconds of (round-trip) latency
        """
        if not self._connected:
            logger.error("Not connected")
            return None

        start = time.monotonic()
        self._send({"action": "ping"})
        self._wait_for("pong")
        return time.monotonic() - start

    def flushdb(self) -> str | None:
        """
        Wipe all topics in the CNS database.

        **WARNING**: This is a destructive operation.

        :return: Server-reported timestamp
        """
        self._send({"action": "flush"})
        return self._wait_for("flush")

    def delete(self, topic: str) -> str | None:
        """
        Delete a CNS topic from the database.
        :param topic: Topic to delete.
        :return: Server-reported timestamp
        """
        key = f"del:{topic}"
        self._send({"action": "del", "topic": topic})
        return self._wait_for(key)

    def set(self, topic: str, data: JSONType, timeout: int | None = None) -> str | None:
        """
        Set the value of a CNS topic.
        :param topic: Topic to set.
        :param data: Data to set.
        :param timeout: Millisecond expiration of the topic.
        :return: Server-reported timestamp
        """
        key = f"set:{topic}"
        if timeout:
            self._send({"action": "set", "topic": topic, "data": data, "timeout": timeout})
        else:
            self._send({"action": "set", "topic": topic, "data": data})
        return self._wait_for(key)

    def get(self, topic: str) -> JSONType | None:
        """
        Get the value of a CNS topic.
        :param topic: Topic to get.
        :return: JSON-serializable data. Returns None if topic is not found, or if the data is null.
        """
        key = f"get:{topic}"
        self._send({"action": "get", "topic": topic})
        return self._wait_for(key)

    def subscribe(self, topic: str, callback: Callable[[str, JSONType], None]):
        """
        Subscribe to a CNS topic.

        **NOTE**: The callback will be called from a background thread. The callback must not block.

        :param topic: Topic to subscribe to.
        :param callback: Callback to execute when a topic changes.
        :return:
        """
        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
            key = f"sub:{topic}"
            self._send({"action": "sub", "topic": topic})
            self._wait_for(key)

        self._subscriptions[topic].append(callback)

    def topic_count(self) -> int:
        """
        Get the number of topics on the CNS Server's database.
        :return: Topic count
        """
        self._send({"action": "tcnt"})
        return self._wait_for("tcnt")

    def topics(self) -> list[str]:
        """
        Get a list of topics in the CNS Server's database.
        :return: List of topics.
        """
        self._send({"action": "topics"})
        return self._wait_for("topics")

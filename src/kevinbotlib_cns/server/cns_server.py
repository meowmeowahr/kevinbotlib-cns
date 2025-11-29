import dataclasses
import datetime
import uuid
from json import JSONDecodeError

from fastapi import FastAPI as _FastAPI
from fastapi.websockets import WebSocket as _WebSocket
from loguru import logger
from uvicorn import Config as _Config, Server as _Server
import threading
import asyncio

from kevinbotlib_cns.server.cns_server_logging import init_logging


@dataclasses.dataclass
class ClientInfo:
    client_uuid: uuid.UUID
    client_id: str
    websocket: _WebSocket

    def __hash__(self):
        return hash((self.client_uuid, self.client_id))


class CNSServer:
    """
    The KevinbotLib CNS (Communication & Networking Service) Server

    Provides simple robot communication.
    """

    def __init__(self):
        self.clients: dict[uuid.UUID, ClientInfo] = {}
        self.subscriptions: dict[str, set[ClientInfo]] = {}
        self.database: dict[str, str] = {}

        self.app: _FastAPI | None = None

        self._server: _Server | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _register_routes(self):
        @self.app.websocket("/cns")
        async def websocket_endpoint(websocket: _WebSocket):
            await websocket.accept()

            client_id: str | None = websocket.headers.get("client-id", None)
            if not client_id:
                logger.warning(
                    "Client did not provide client id (Client-ID), connection ending."
                )
                await websocket.close(
                    1002, "The client did not provide a valid client id (Client-ID)"
                )
                return
            logger.info(f"Client connected with ID: {client_id}")

            client_uuid = uuid.uuid4()
            logger.info(f"Client {client_id} UUID is {client_uuid}")

            client_info = ClientInfo(client_uuid, client_id, websocket)
            self.clients[client_uuid] = client_info

            while True:
                try:
                    data = await websocket.receive_json()

                    match data.get("action"):
                        case "set":
                            if "topic" not in data:
                                await websocket.send_json(
                                    {
                                        "error": "Didn't get a topic for 'set' action. A topic must be provided."
                                    }
                                )
                                continue
                            if "data" not in data:
                                await websocket.send_json(
                                    {
                                        "error": "Didn't get data for 'set' action. Use the 'del' action if you intend to delete the topic."
                                    }
                                )
                                continue
                            self.database[data["topic"]] = data["data"]

                            if data["topic"] in self.subscriptions:
                                for client in self.subscriptions[data["topic"]]:
                                    await client.websocket.send_json(
                                        {
                                            "action": "pub",
                                            "topic": data["topic"],
                                            "data": data["data"],
                                        }
                                    )

                            await websocket.send_json(
                                {
                                    "action": "set",
                                    "topic": data["topic"],
                                    "ts": datetime.datetime.now(
                                        tz=datetime.timezone.utc
                                    ).isoformat(),
                                }
                            )

                            logger.debug(
                                f"{client_id} set topic: {data['topic']} to {data['data']}"
                            )
                        case "get":
                            if "topic" not in data:
                                await websocket.send_json(
                                    {
                                        "error": "Didn't get a topic for 'get' action. A topic must be provided."
                                    }
                                )
                                continue
                            await websocket.send_json(
                                {
                                    "action": "get",
                                    "topic": data["topic"],
                                    "data": data["data"],
                                }
                            )

                            logger.debug(f"{client_id} got topic: {data['topic']}")
                        case "sub":
                            if "topic" not in data:
                                await websocket.send_json(
                                    {
                                        "error": "Didn't get a topic for 'sub' action. A topic must be provided."
                                    }
                                )
                                continue
                            if data["topic"] not in self.subscriptions:
                                self.subscriptions[data["topic"]] = set()
                            self.subscriptions[data["topic"]].add(client_info)
                            await websocket.send_json(
                                {
                                    "action": "sub",
                                    "topic": data["topic"],
                                    "ts": datetime.datetime.now(
                                        tz=datetime.timezone.utc
                                    ).isoformat(),
                                }
                            )

                            logger.debug(
                                f"{client_id} subscribed to topic: {data['topic']}"
                            )

                except JSONDecodeError as e:
                    logger.warning(f"Client {client_id} received invalid JSON: {e}")
                    await websocket.close(1002, "The client did not provide valid JSON")
                    logger.warning(f"Closing connection to client {client_id}")
                    return

    def start(self, host="0.0.0.0", port=4800):
        """
        Start the KevinbotLib CNS Server in a background thread.

        :param host: Host to serve on.
        :param port: Port to serve on.
        :return:
        """
        self.app = _FastAPI()
        self._register_routes()

        def _run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

            config = _Config(
                app=self.app,
                host=host,
                port=port,
                loop="asyncio",
                lifespan="auto",
            )

            self._server = _Server(config)
            init_logging()
            self._loop.run_until_complete(self._server.serve())

        self._thread = threading.Thread(
            target=_run, daemon=True, name="KevinbotLibCNS.Server"
        )
        self._thread.start()

    def stop(self):
        """
        Stop the CNS server cleanly.

        :return:
        """

        if not self._server or not self._loop:
            return

        # Tell uvicorn to shut down
        self._server.should_exit = True

        # Let the event loop finish
        self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread:
            self._thread.join(timeout=2.0)

        self._server = None
        self._loop = None
        self._thread = None

import dataclasses
import datetime
import uuid
from json import JSONDecodeError
from pathlib import Path

from fastapi import FastAPI as _FastAPI
from fastapi.responses import HTMLResponse as _HTMLResponse, JSONResponse as _JSONResponse
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
    connected_at: datetime.datetime = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

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
        self.start_time = datetime.datetime.now(datetime.timezone.utc)

        self.app: _FastAPI | None = None

        self._server: _Server | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _build_topic_tree(self):
        tree = {}

        for topic in self.database.keys():
            parts = topic.split("/")
            current = tree

            for i, part in enumerate(parts):
                if part not in current:
                    current[part] = {
                        "_children": {},
                        "_is_leaf": False,
                        "_topic": None,
                        "_subs": 0,
                    }

                if i == len(parts) - 1:
                    current[part]["_is_leaf"] = True
                    current[part]["_topic"] = topic
                    current[part]["_subs"] = len(self.subscriptions.get(topic, set()))

                current = current[part]["_children"]

        return tree

    def _render_tree(self, tree, depth=0):
        html = ""

        for key, value in sorted(tree.items()):
            indent = depth * 20
            is_leaf = value["_is_leaf"]
            has_children = len(value["_children"]) > 0

            if is_leaf:
                html += f"""
                <div class="tree-node tree-leaf" style="margin-left: {indent}px;" data-topic="{value["_topic"]}" onclick="_loadTopicData('{value["_topic"]}')">
                    <span class="tree-icon">📄</span>
                    <span class="tree-label">{key}</span>
                    <span class="tree-subs">{value["_subs"]} sub(s)</span>
                </div>
                """
            else:
                html += f"""
                <div class="tree-node tree-folder" style="margin-left: {indent}px;">
                    <span class="tree-icon">📁</span>
                    <span class="tree-label">{key}/</span>
                </div>
                """

            if has_children:
                html += self._render_tree(value["_children"], depth + 1)

        return html

    @staticmethod
    def _error_response(code: int = 500, desc: str = "Internal Server Error"):
        return _HTMLResponse(
            content=f"<h1>{code} {desc}</h1><hr><p>Something went wrong.</p>",
            status_code=code,
        )

    @staticmethod
    def _load_dashboard_template():
        template_path = Path(__file__).parent / "dashboard_template.html"

        if template_path.exists():
            with open(template_path, "r", encoding="utf-8") as f:
                return f.read()
        else:
            logger.error(f"Dashboard template not found at {template_path}")
            return None

    def _register_routes(self):
        @self.app.get("/", response_class=_HTMLResponse)
        async def dashboard():
            uptime = datetime.datetime.now(datetime.timezone.utc) - self.start_time
            uptime_str = str(uptime).split(".")[0]

            # client table
            clients_html = ""
            for client_uuid, client_info in self.clients.items():
                connected_duration = (
                    datetime.datetime.now(datetime.timezone.utc)
                    - client_info.connected_at
                )
                connected_str = str(connected_duration).split(".")[0]
                clients_html += f"""
                <tr>
                    <td>{client_info.client_id}</td>
                    <td><code>{str(client_uuid)[:8]}...</code></td>
                    <td>{connected_str}</td>
                </tr>
                """

            if not clients_html:
                clients_html = "<tr><td colspan='3' class='empty-state'>No clients connected</td></tr>"

            # topic tree
            if self.database:
                tree = self._build_topic_tree()
                topic_tree_html = self._render_tree(tree)
            else:
                topic_tree_html = "<div class='empty-state'>No topics in database</div>"

            # subs table
            subscriptions_html = ""
            for topic, subscribers in self.subscriptions.items():
                for client in subscribers:
                    subscriptions_html += f"""
                    <tr>
                        <td><code>{topic}</code></td>
                        <td>{client.client_id}</td>
                        <td><code>{str(client.client_uuid)[:8]}...</code></td>
                    </tr>
                    """

            if not subscriptions_html:
                subscriptions_html = "<tr><td colspan='3' class='empty-state'>No active subscriptions</td></tr>"

            template = self._load_dashboard_template()
            if not template:
                return self._error_response(500, "Internal Server Error")
            html_content = template.replace("{{client_count}}", str(len(self.clients)))
            html_content = html_content.replace(
                "{{topic_count}}", str(len(self.database))
            )
            html_content = html_content.replace(
                "{{subscription_count}}",
                str(sum(len(subs) for subs in self.subscriptions.values())),
            )
            html_content = html_content.replace("{{uptime}}", uptime_str)
            html_content = html_content.replace("{{topic_tree}}", topic_tree_html)
            html_content = html_content.replace("{{clients_table}}", clients_html)
            html_content = html_content.replace(
                "{{subscriptions_table}}", subscriptions_html
            )

            return _HTMLResponse(content=html_content)

        @self.app.get("/api/dashboard-data")
        async def get_dashboard_data():
            uptime = datetime.datetime.now(datetime.timezone.utc) - self.start_time
            uptime_str = str(uptime).split(".")[0]

            # clients list
            clients = []
            for client_uuid, client_info in self.clients.items():
                connected_duration = (
                    datetime.datetime.now(datetime.timezone.utc)
                    - client_info.connected_at
                )
                connected_str = str(connected_duration).split(".")[0]
                clients.append(
                    {
                        "id": client_info.client_id,
                        "uuid": str(client_uuid)[:8] + "...",
                        "connected_for": connected_str,
                    }
                )

            # topic tree
            if self.database:
                tree = self._build_topic_tree()
                topic_tree_html = self._render_tree(tree)
            else:
                topic_tree_html = "<div class='empty-state'>No topics in database</div>"

            # subs list
            subscriptions = []
            for topic, subscribers in self.subscriptions.items():
                for client in subscribers:
                    subscriptions.append(
                        {
                            "topic": topic,
                            "client_id": client.client_id,
                            "client_uuid": str(client.client_uuid)[:8] + "...",
                        }
                    )

            return _JSONResponse(
                {
                    "stats": {
                        "client_count": len(self.clients),
                        "topic_count": len(self.database),
                        "subscription_count": sum(
                            len(subs) for subs in self.subscriptions.values()
                        ),
                        "uptime": uptime_str,
                    },
                    "topic_tree_html": topic_tree_html,
                    "clients": clients,
                    "subscriptions": subscriptions,
                }
            )

        @self.app.get("/api/topic/{topic:path}")
        async def get_topic_data(topic: str):
            data = self.database.get(topic)
            subscribers = len(self.subscriptions.get(topic, set()))

            return _JSONResponse(
                {"topic": topic, "data": data, "subscribers": subscribers}
            )

        @self.app.websocket("/cns")
        async def websocket_endpoint(websocket: _WebSocket):
            await websocket.accept()

            client_id: str | None = websocket.headers.get("client-id", None)
            if not client_id:
                logger.warning(
                    "Client did not provide client id (Client-ID), closing connection."
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

            try:
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
                                        "data": self.database.get(data["topic"]),
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
                            case "tcnt":
                                await websocket.send_json(
                                    {
                                        "action": "tcnt",
                                        "count": len(self.database)
                                    }
                                )

                                logger.debug(
                                    f"{client_id} request for topic count: {len(self.database)}"
                                )
                            case "topics":
                                await websocket.send_json(
                                    {
                                        "action": "topics",
                                        "topics": list(self.database.keys())
                                    }
                                )

                                logger.debug(
                                    f"{client_id} request for topics list: {list(self.database.keys())}"
                                )

                    except JSONDecodeError as e:
                        logger.warning(f"Client {client_id} received invalid JSON: {e}")
                        await websocket.close(
                            1002, "The client did not provide valid JSON"
                        )
                        logger.warning(f"Closing connection to client {client_id}")
                        break
            finally:
                if client_uuid in self.clients:
                    del self.clients[client_uuid]
                    logger.info(f"Client {client_id} ({client_uuid}) disconnected")

                for topic_subs in self.subscriptions.values():
                    topic_subs.discard(client_info)

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
        # noinspection HttpUrlsUsage
        logger.info(f"CNS Server dashboard available at http://{host}:{port}/")

    def stop(self):
        """
        Stop the CNS server cleanly.

        :return:
        """

        if not self._server or not self._loop:
            return

        # Tell uvicorn to shut down
        self._server.should_exit = True
        def stop_loop(_: object): # maybe a bug in PyCharm's linter?
            self._loop.stop()
        self._loop.call_soon_threadsafe(stop_loop, None)

        if self._thread:
            self._thread.join(timeout=2.0)

        self._server = None
        self._loop = None
        self._thread = None

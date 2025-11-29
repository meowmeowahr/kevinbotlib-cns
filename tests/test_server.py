import threading

import requests

import time
from kevinbotlib_cns.server.cns_server import CNSServer


def test_server_start_stop():
    server = CNSServer()
    server.start(port=4801)
    assert "KevinbotLibCNS.Server" in [t.name for t in threading.enumerate()]
    time.sleep(1)
    server.stop()
    assert "KevinbotLibCNS.Server" not in [t.name for t in threading.enumerate()]

def test_server_dashboard_navigate():
    server = CNSServer()
    server.start(port=4801)
    assert "KevinbotLibCNS.Server" in [t.name for t in threading.enumerate()]
    time.sleep(1)

    requests.get("http://127.0.0.1:4801/").raise_for_status()

    # inject data
    server.database["pytest"] = "foo"

    requests.get("http://127.0.0.1:4801/").raise_for_status()

    server.stop()
    assert "KevinbotLibCNS.Server" not in [t.name for t in threading.enumerate()]

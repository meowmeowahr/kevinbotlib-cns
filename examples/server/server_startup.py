import sys
import time

from loguru import logger

from kevinbotlib_cns.server import CNSServer

logger.remove()
logger.add(sys.stderr, level="INFO") # DEBUG logs are intensive

if __name__ == "__main__":
    server = CNSServer()
    server.start(host="0.0.0.0", port=4800)

    while True:
        time.sleep(1)

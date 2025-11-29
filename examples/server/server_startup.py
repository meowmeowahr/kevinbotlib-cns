import time

from kevinbotlib_cns.server import CNSServer

if __name__ == "__main__":
    server = CNSServer()
    server.start(host="0.0.0.0", port=4800)

    while True:
        time.sleep(1)

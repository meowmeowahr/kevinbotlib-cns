import time

import kevinbotlib_cns.client
from kevinbotlib_cns.client import CNSClient

if __name__ == '__main__':
    client = CNSClient("ws://127.0.0.1:4800/cns", "my-client")
    client.connect()

    count = 0
    while True:
        print(f"count: {count}")
        client.set("example/count", count)

        if not client._async_client._connected:
            try:
                client.connect()
            except ConnectionError:
                pass

        time.sleep(1)
        count += 1
import asyncio

from kevinbotlib_cns.client.async_client import CNSAsyncClient
from kevinbotlib_cns.types import JSONType


def callback(topic: str, value: JSONType):
    print(f"{topic} was set to {value}")


async def run_example():
    client = CNSAsyncClient("ws://127.0.0.1:4800/cns", "my-client")
    await client.connect()
    print("Subscribing to robot/battery")
    await client.subscribe("robot/battery", callback)
    print("Setting robot/battery to 12.0")
    await client.set("robot/battery", 12.0)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_example())

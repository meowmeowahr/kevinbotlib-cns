import asyncio

from kevinbotlib_cns.client.async_client import CNSAsyncClient


async def run_example():
    client = CNSAsyncClient("ws://127.0.0.1:4800/cns", "my-client")
    await client.connect()
    print("Setting robot/battery to 12.0")
    await client.set("robot/battery", 12.0)
    print(f"robot/battery is {await client.get('robot/battery')}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_example())

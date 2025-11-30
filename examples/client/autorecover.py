import asyncio

from kevinbotlib_cns.client.async_client import CNSAsyncClient


async def run_example():
    client = CNSAsyncClient("ws://127.0.0.1:4800/cns", "my-client")
    await client.connect()

    count = 0
    while True:
        print(f"Count is {count}")
        await client.set("example/count", count)
        print(f"example/count is {await client.get('example/count')}")

        if not client._connected:
            try:
                await client.connect()
            except ConnectionRefusedError:
                pass

        await asyncio.sleep(1)
        count += 1


if __name__ == "__main__":
    asyncio.run(run_example())

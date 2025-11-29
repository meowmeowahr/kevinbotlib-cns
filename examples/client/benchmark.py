import asyncio
import time
from kevinbotlib_cns.client.async_client import CNSAsyncClient

TESTS = [
    256,  # 256 bytes
    2048,  # 2Kb
    65536,  # 64Kb
    1048576,  # 1Mb
    4194304,  # 4Mb
    1310720,  # 10Mb
]


async def run_benchmark():
    client = CNSAsyncClient("ws://127.0.0.1:4800/cns", "benchmark-client")
    await client.connect()
    await asyncio.sleep(0.1)  # settle down logging

    for size in TESTS:
        print(f"=== TESTING {size} byte payload ===")
        large_string = "A" * size

        print(f"Generated test payload: {len(large_string):,} bytes")

        # ---- SET ----
        t0 = time.perf_counter()
        await client.set("bench/blob", large_string)
        t1 = time.perf_counter()

        set_time = t1 - t0
        print(
            f"SET time: {set_time:.4f} sec "
            f"({len(large_string) / set_time / 1e6:.2f} MB/s)"
        )

        # ---- GET ----
        t0 = time.perf_counter()
        received = await client.get("bench/blob")
        t1 = time.perf_counter()

        get_time = t1 - t0
        print(
            f"GET time: {get_time:.4f} sec ({len(received) / get_time / 1e6:.2f} MB/s)"
        )

        print(f"Payload integrity OK: {len(received) == len(large_string)}")

    await asyncio.sleep(0.1)  # settle down logging
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_benchmark())

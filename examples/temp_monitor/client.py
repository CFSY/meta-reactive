# Client implementation
import asyncio
import json

import aiohttp


async def run_client():
    async def process_stream():
        async with aiohttp.ClientSession() as session:
            # Create stream
            print("Creating stream...")
            async with session.post(
                "http://localhost:1234/v1/streams/monitor", json={"threshold": 30.0}
            ) as response:
                stream_data = await response.json()
                stream_id = stream_data["instance_id"]
                print(f"Stream created with ID: {stream_id}")

            # Connect to stream
            print("Connecting to stream...")
            async with session.get(
                f"http://localhost:1234/v1/streams/{stream_id}",
                headers={
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            ) as response:
                buffer = ""
                async for line in response.content:
                    line = line.decode("utf-8")
                    buffer += line

                    if buffer.endswith("\n\n"):
                        for message in buffer.strip().split("\n\n"):
                            for field in message.split("\n"):
                                if field.startswith("data:"):
                                    try:
                                        data = json.loads(field[5:].strip())
                                        print(
                                            f"Received update: {json.dumps(data, indent=2)}"
                                        )
                                    except json.JSONDecodeError as e:
                                        print(f"Error parsing JSON: {e}")
                        buffer = ""

    try:
        await process_stream()
    except aiohttp.ClientError as e:
        print(f"Client connection error: {e}")
    except Exception as e:
        print(f"Client error: {e}")


if __name__ == "__main__":
    asyncio.run(run_client())

# Client implementation
import json

import aiohttp

from examples.utils.colored_logger import setup_logger

client_logger = setup_logger("client", "CLIENT")


async def run_client():
    async def process_stream():
        async with aiohttp.ClientSession() as session:
            # Create stream
            client_logger.info("Creating stream...")
            async with session.post(
                "http://localhost:1234/v1/streams/monitor", json={"threshold": 30.0}
            ) as response:
                stream_data = await response.json()
                stream_id = stream_data["instance_id"]
                client_logger.info(f"Stream created with ID: {stream_id}")

            # Connect to stream
            client_logger.info("Connecting to stream...")
            async with session.get(
                f"http://localhost:1234/v1/streams/{stream_id}",
                headers={"Accept": "text/event-stream"},
            ) as response:
                async for line in response.content:
                    line = line.decode("utf-8")
                    if line.startswith("data:"):
                        data = json.loads(line[5:])
                        client_logger.info(
                            f"Received update: {json.dumps(data, indent=2)}"
                        )

                        # for sensor_id, alert in data.items():
                        #     client_logger.info(f"  {sensor_id}: {alert}")

    try:
        await process_stream()
    except Exception as e:
        client_logger.error(f"Client error: {e}")

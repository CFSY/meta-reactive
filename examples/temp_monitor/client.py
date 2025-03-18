# Client implementation
import asyncio
import json
import time

import aiohttp


def format_alert(data):
    """Format temperature alert data for display"""

    alert_symbols = {"normal": "✓", "low": "❄️", "high": "🔥", "critical": "⚠️"}

    alert_colors = {
        "normal": "\033[92m",  # Green
        "low": "\033[94m",  # Blue
        "high": "\033[93m",  # Yellow
        "critical": "\033[91m",  # Red
    }

    reset_color = "\033[0m"

    # Default values if keys are missing
    location = data.get("location", "unknown")
    temp = data.get("temperature", "N/A")
    alert_level = data.get("alert_level", "normal")
    details = data.get("details", "No details available")

    symbol = alert_symbols.get(alert_level, "?")
    color = alert_colors.get(alert_level, "\033[0m")

    return f"{color}{symbol} [{location}] {temp}°C - {details}{reset_color}"


async def run_client():
    async def process_stream():
        async with aiohttp.ClientSession() as session:
            # Create stream
            print("Creating stream...")
            async with session.post(
                "http://localhost:1234/v1/streams/temperature_monitor",
                json={"threshold": 40.0},
            ) as response:
                stream_data = await response.json()
                stream_id = stream_data["instance_id"]
                print(f"Stream created with ID: {stream_id}")

            # Connect to stream
            print("Connecting to stream...")
            print("\nTemperature Monitor - Live Alerts")
            print("=================================")

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

                                        # Handle initial data differently
                                        if (
                                            "event" in message
                                            and "event: init" in message
                                        ):
                                            print("\nInitial state:")
                                            for item in data:
                                                if item and len(item) > 1:
                                                    sensor_id = item[0]

                                                    sensor_data = item[1]
                                                    if sensor_data:
                                                        timestamp = time.strftime(
                                                            "%H:%M:%S"
                                                        )
                                                        print(
                                                            f"[{timestamp}] {sensor_id}: {format_alert(sensor_data)}"
                                                        )
                                        else:
                                            # Handle updates
                                            print("\nUpdated state:")
                                            for item in data:
                                                if item and len(item) > 1:
                                                    sensor_id = item[0]
                                                    updates = item[1]
                                                    if updates and len(updates) > 0:
                                                        # Print with timestamp
                                                        timestamp = time.strftime(
                                                            "%H:%M:%S"
                                                        )
                                                        print(
                                                            f"[{timestamp}] {sensor_id}: {format_alert(updates[0])}"
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

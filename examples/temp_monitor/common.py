import asyncio
import random
from datetime import datetime
from typing import Dict, Optional, Callable, Awaitable

from pydantic import BaseModel


# Data Models
class SensorReading(BaseModel):
    sensor_id: str
    location: str
    temperature: float
    timestamp: datetime


class LocationInfo(BaseModel):
    min_temp: float
    max_temp: float
    description: str


# Alert level evaluation logic
def evaluate_temperature_alert(
    avg_temp: float, location: str, location_info: LocationInfo, global_threshold: float
) -> Dict[str, str]:
    """Evaluate temperature reading and generate appropriate alert data"""
    min_temp, max_temp = location_info.min_temp, location_info.max_temp

    alert_level = "normal"
    details = f"Temperature {avg_temp:.1f}°C is within normal range ({min_temp:.1f}-{max_temp:.1f}°C) for {location}"

    # Check if temperature is outside the expected range
    if avg_temp < min_temp:
        alert_level = "low"
        details = f"Temperature {avg_temp:.1f}°C below normal range ({min_temp:.1f}-{max_temp:.1f}°C) for {location}"
    elif avg_temp > max_temp:
        alert_level = "high"
        details = f"Temperature {avg_temp:.1f}°C above normal range ({min_temp:.1f}-{max_temp:.1f}°C) for {location}"

        # Check global threshold for critical alerts
        if avg_temp > global_threshold:
            alert_level = "critical"
            details = f"CRITICAL: Temperature {avg_temp:.1f}°C significantly above normal range and threshold ({global_threshold}°C) for {location}"

    return {
        "temperature": f"{avg_temp:.1f}",
        "location": location,
        "alert_level": alert_level,
        "details": details,
    }


# Simulation utilities
def generate_temperature_for_location(location: str) -> float:
    """Generate temperature data based on location"""
    if location == "server_room":
        # Server room runs cooler normally, but can spike
        if random.random() < 0.3:  # 30% chance of spike
            return random.uniform(25.0, 35.0)  # Temperature spike
        else:
            return random.uniform(18.0, 22.0)  # Normal range
    elif location == "warehouse":
        # Warehouse has wider temperature swings
        return random.uniform(10.0, 40.0)
    else:
        # Office has more consistent temperature
        return random.uniform(19.0, 26.0)


# Default reference data
DEFAULT_LOCATIONS = {
    "office": LocationInfo(min_temp=20.0, max_temp=25.0, description="Office area"),
    "server_room": LocationInfo(
        min_temp=18.0, max_temp=22.0, description="Server room"
    ),
    "warehouse": LocationInfo(
        min_temp=20.0, max_temp=30.0, description="Warehouse area"
    ),
}


# Default sensor configuration
DEFAULT_SENSORS = [
    ("sensor1", "office"),
    ("sensor2", "server_room"),
    ("sensor3", "warehouse"),
]


async def run_temperature_simulation(
    reading_updater: Callable[[str, str, float], Awaitable[None]],
    location_updater: Optional[Callable[[str, LocationInfo], Awaitable[None]]] = None,
    sensors=DEFAULT_SENSORS,
    interval: float = 2.0,
) -> None:
    """
    Run temperature simulation

    Args:
        reading_updater: Async function to call with (sensor_id, location, temperature)
        location_updater: Optional function to update location reference data
        sensors: List of (sensor_id, location) tuples to simulate
        interval: Time between readings in seconds
    """
    while True:
        # Generate and report new sensor readings
        for sensor_id, location in sensors:
            temp = generate_temperature_for_location(location)
            await reading_updater(sensor_id, location, temp)
            print(
                f"New reading - Sensor: {sensor_id} ({location}), Temperature: {temp:.1f}°C"
            )

        # Occasional updates to reference data if updater provided
        if location_updater and random.random() < 0.5:  # 50% chance each cycle
            location = random.choice([loc for _, loc in sensors])

            # Get current reference data (implementation-specific)
            current = DEFAULT_LOCATIONS[location]  # Default fallback

            # Slight adjustment to expected temperature ranges
            new_min = max(10, current.min_temp + random.uniform(-1, 1))
            new_max = max(new_min + 2, current.max_temp + random.uniform(-1, 1))

            new_info = LocationInfo(
                min_temp=new_min,
                max_temp=new_max,
                description=current.description,
            )

            await location_updater(location, new_info)
            print(
                f"Updated reference data for {location}: {new_min:.1f}-{new_max:.1f}°C"
            )

        await asyncio.sleep(interval)

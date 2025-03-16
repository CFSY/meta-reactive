import asyncio
import random
from datetime import datetime
from typing import Dict, List, Tuple

from pydantic import BaseModel

from src.reactive_framework.classic.mapper import (
    OneToOneMapper,
    ManyToOneMapper,
)
from src.reactive_framework.classic.resource import Resource, ResourceParams
from src.reactive_framework.classic.service import Service
from src.reactive_framework.core.compute_graph import ComputedCollection


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


# Mappers
class AverageTemperatureMapper(ManyToOneMapper[str, SensorReading, Tuple[float, str]]):
    """Computes average temperature for each sensor from its readings"""

    def map_values(self, readings: list[SensorReading]) -> Tuple[float, str]:
        if not readings:
            return 0.0, ""
        avg_temp = sum(r.temperature for r in readings) / len(readings)
        # Return both temperature and location
        return avg_temp, readings[0].location


class EnhancedAlertMapper(OneToOneMapper[str, Tuple[float, str], Dict[str, str]]):
    """
    Generates enhanced alerts by comparing current temperatures
    with reference data for each location
    """

    def __init__(
        self, location_references: ComputedCollection, global_threshold: float
    ):
        self.location_references = location_references
        self.global_threshold = global_threshold

    def map_value(self, value: Tuple[float, str]) -> Dict[str, str]:
        avg_temp, location = value

        # Get reference data for this location
        location_info = self.location_references.get(location)
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
            if avg_temp > self.global_threshold:
                alert_level = "critical"
                details = f"CRITICAL: Temperature {avg_temp:.1f}°C significantly above normal range and threshold ({self.global_threshold}°C) for {location}"

        return {
            "temperature": f"{avg_temp:.1f}",
            "location": location,
            "alert_level": alert_level,
            "details": details,
        }


# Resource Parameters
class MonitorParams(ResourceParams):
    threshold: float = 30.0  # Global alert threshold in Celsius


# Resource Implementation
class TemperatureMonitorResource(Resource[str, dict]):
    def __init__(
        self, name: str, readings_collection, location_references, compute_graph
    ):
        super().__init__(name, MonitorParams, compute_graph)
        self.readings = readings_collection
        self.location_references = location_references
        print("RESOURCE INIT:", self.name)

    def setup_resource_collection(self, params: MonitorParams):
        # Compute average temperatures
        averages = self.readings.map(AverageTemperatureMapper)

        # Generate enhanced alerts using both averages and location references
        alerts = averages.map(
            EnhancedAlertMapper,
            self.location_references,  # Pass the location references collection
            params.threshold,  # Pass the global threshold
        )

        return alerts


async def main():
    # Initialize service
    service = Service("temperature_monitor", port=1234)

    # Create a collection for sensor readings
    readings = ComputedCollection[str, List[SensorReading]](
        "sensor_readings", service.compute_graph
    )

    # Create a collection for location reference data
    location_references = ComputedCollection[str, LocationInfo](
        "location_references", service.compute_graph
    )

    # Populate location reference data
    location_references.set(
        "office", LocationInfo(min_temp=20.0, max_temp=25.0, description="Office area")
    )
    location_references.set(
        "server_room",
        LocationInfo(min_temp=18.0, max_temp=22.0, description="Server room"),
    )
    location_references.set(
        "warehouse",
        LocationInfo(min_temp=20.0, max_temp=30.0, description="Warehouse area"),
    )

    # Create and add resource
    monitor = TemperatureMonitorResource(
        "temperature_monitor", readings, location_references, service.compute_graph
    )
    service.add_resource("monitor", monitor)

    # Simulate sensor readings
    async def simulate_sensors():
        sensors = [
            ("sensor1", "office"),
            ("sensor2", "server_room"),
            ("sensor3", "warehouse"),
        ]

        while True:
            for sensor_id, location in sensors:
                # Generate temperature with some randomness based on location

                # Add location-specific variations
                if location == "server_room":
                    # Server room runs cooler normally, but can spike
                    if random.random() < 0.3:  # 30% chance of spike
                        temp = random.uniform(25.0, 35.0)  # Temperature spike
                    else:
                        temp = random.uniform(18.0, 22.0)  # Normal range
                elif location == "warehouse":
                    # Warehouse has wider temperature swings
                    temp = random.uniform(10.0, 40.0)
                else:
                    # Office has more consistent temperature
                    temp = random.uniform(19.0, 26.0)

                reading = SensorReading(
                    sensor_id=sensor_id,
                    location=location,
                    temperature=temp,
                    timestamp=datetime.now(),
                )

                old_readings = readings.get(sensor_id)

                # Create new readings list
                if old_readings is None:
                    new_readings = [reading]
                else:
                    new_readings = old_readings + [reading]
                    # Keep only the latest 10 readings
                    new_readings = new_readings[-10:]

                readings.set(sensor_id, new_readings)

                print(
                    f"New reading - Sensor: {sensor_id} ({location}), Temperature: {temp:.1f}°C"
                )

            # Occasional updates to reference data to demonstrate reactivity
            if random.random() < 0.5:  # 50% chance each cycle
                location = random.choice(["office", "server_room", "warehouse"])
                current = location_references.get(location)

                # Slight adjustment to expected temperature ranges
                new_min = max(10, current.min_temp + random.uniform(-1, 1))
                new_max = max(new_min + 2, current.max_temp + random.uniform(-1, 1))
                location_references.set(
                    location,
                    LocationInfo(
                        min_temp=new_min,
                        max_temp=new_max,
                        description=current.description,
                    ),
                )
                print(
                    f"Updated reference data for {location}: {new_min:.1f}-{new_max:.1f}°C"
                )

            await asyncio.sleep(2)

    service_task, simulation_task = None, None

    try:
        # Create tasks for both the service and simulation
        service_task = asyncio.create_task(service.start())
        simulation_task = asyncio.create_task(simulate_sensors())

        # Wait for both tasks
        await asyncio.gather(service_task, simulation_task)
    except KeyboardInterrupt:
        if simulation_task:
            simulation_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())

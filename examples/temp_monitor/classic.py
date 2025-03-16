import asyncio
import random
from datetime import datetime
from typing import List

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
    temperature: float
    timestamp: datetime


# Mappers
class AverageTemperatureMapper(ManyToOneMapper[str, SensorReading, float]):
    """Computes average temperature for each sensor from its readings"""

    def map_values(self, readings: list[SensorReading]) -> float:
        if not readings:
            return 0.0
        res = sum(r.temperature for r in readings) / len(readings)
        return res


class TemperatureAlertMapper(OneToOneMapper[str, float, str]):
    """Generates alerts for high temperatures"""

    def __init__(self, threshold: float):
        self.threshold = threshold

    def map_value(self, avg_temp: float) -> str:
        print(f"ALERT MAPPER: avg={avg_temp}")

        if avg_temp > self.threshold:
            return f"ALERT: Temperature {avg_temp:.1f}°C exceeds threshold {self.threshold}°C"
        return f"Normal: Temperature {avg_temp:.1f}°C"


# Resource Parameters
class MonitorParams(ResourceParams):
    threshold: float = 30.0  # Alert threshold in Celsius


# Resource Implementation
class TemperatureMonitorResource(Resource[str, dict]):
    def __init__(self, name: str, readings_collection, compute_graph):
        super().__init__(name, MonitorParams, compute_graph)
        self.readings: ComputedCollection = readings_collection
        print("RESOURCE INIT:", self.name)

    def setup_resource_collection(self, params: MonitorParams):
        # First, compute average temperatures using the new map method
        averages = self.readings.map(
            AverageTemperatureMapper(), f"{self.name}_averages"
        )

        # Then, generate alerts based on averages using the new map method
        alerts = averages.map(
            TemperatureAlertMapper(params.threshold), f"{self.name}_alerts"
        )

        return alerts


async def main():
    # Initialize service
    service = Service("temperature_monitor", port=1234)

    # Create a collection for sensor readings
    readings = ComputedCollection[str, List[SensorReading]](
        "sensor_readings", service.compute_graph
    )

    # Create and add resource
    monitor = TemperatureMonitorResource(
        "temperature_monitor", readings, service.compute_graph
    )
    service.add_resource("monitor", monitor)

    # Simulate sensor readings
    async def simulate_sensors():
        sensors = ["sensor1"]
        while True:
            for sensor_id in sensors:
                reading = SensorReading(
                    sensor_id=sensor_id,
                    temperature=random.uniform(25.0, 35.0),
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

                print("New reading - Sensor:", sensor_id, reading.temperature)
            await asyncio.sleep(2)

    service_task, simulation_task = None, None

    try:
        # Create tasks for both the service and simulation
        service_task = asyncio.create_task(service.start())
        simulation_task = asyncio.create_task(simulate_sensors())

        # Wait for both tasks
        await asyncio.gather(service_task, simulation_task)
    except KeyboardInterrupt:
        simulation_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())

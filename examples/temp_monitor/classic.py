import asyncio
import random
from datetime import datetime
from typing import List

from pydantic import BaseModel

from examples.temp_monitor.client import run_client
from examples.utils.colored_logger import setup_logger
from src.reactive_framework.classic.mapper import (
    OneToOneMapper,
    ManyToOneMapper,
    create_mapped_collection,
)
from src.reactive_framework.classic.resource import Resource, ResourceParams
from src.reactive_framework.classic.service import Service
from src.reactive_framework.core.collection import Collection

server_logger = setup_logger("server", "SERVER")
sensor_logger = setup_logger("sensor", "INFO")


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
        self.readings = readings_collection
        print("RESOURCE INIT:", self.name)

    def create_collection(self, params: MonitorParams):
        # First, compute average temperatures
        averages = create_mapped_collection(
            self.readings,
            AverageTemperatureMapper(),
            self.compute_graph,
            f"{self.name}_averages",
        )

        # Then, generate alerts based on averages
        alerts = create_mapped_collection(
            averages,
            TemperatureAlertMapper(params.threshold),
            self.compute_graph,
            f"{self.name}_alerts",
        )

        return alerts

    def setup_dependencies(self, collection, params: MonitorParams):
        self.compute_graph.add_dependency(collection, self.readings)


async def main():
    # Initialize service
    service = Service("temperature_monitor", port=1234)

    # Create a collection for sensor readings
    readings = Collection[str, List[SensorReading]]("sensor_readings")
    service.add_collection("readings", readings)

    # Create and add resource
    monitor = TemperatureMonitorResource(
        "temperature_monitor", readings, service.compute_graph
    )
    service.add_resource("monitor", monitor)

    # Start service
    await service.start()
    server_logger.info("Service started")

    # Start client
    client_task = asyncio.create_task(run_client())

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
                new_readings = (
                    [reading] if old_readings is None else old_readings + [reading]
                )
                readings.set(sensor_id, new_readings)
                sensor_logger.info(
                    f"New reading - Sensor: {sensor_id}, "
                    f"Temperature: {reading.temperature:.1f}°C"
                )
            await asyncio.sleep(2)

    # Start simulation
    simulation_task = asyncio.create_task(simulate_sensors())

    try:
        # Keep service running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        server_logger.info("Shutting down...")
        simulation_task.cancel()
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())

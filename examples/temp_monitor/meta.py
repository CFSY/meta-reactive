import asyncio
import random
from datetime import datetime

from pydantic import BaseModel

from examples.temp_monitor.client import run_client
from src.reactive_framework.meta.decorators import collection, resource, computed
from src.reactive_framework.meta.service import MetaService


# Data Models
class SensorReading(BaseModel):
    sensor_id: str
    temperature: float
    timestamp: datetime


# Collections
@collection(name="readings")
class SensorReadingsCollection:
    def __init__(self, name: str, compute_graph):
        self.name = name
        self._readings: dict[str, SensorReading] = {}

    @computed
    def average_temperatures(self) -> dict[str, float]:
        result = {}
        for sensor_id, reading in self._readings.items():
            result[sensor_id] = reading.temperature
        return result

    @computed
    def temperature_alerts(self, threshold: float = 30.0) -> dict[str, str]:
        result = {}
        for sensor_id, avg_temp in self.average_temperatures.items():
            if avg_temp > threshold:
                result[sensor_id] = (
                    f"ALERT: Temperature {avg_temp:.1f}°C exceeds threshold {threshold}°C"
                )
            else:
                result[sensor_id] = f"Normal: Temperature {avg_temp:.1f}°C"
        return result


# Resource
@resource(name="monitor")
class TemperatureMonitorResource:
    def __init__(self, threshold: float = 30.0):
        self.threshold = threshold
        self._readings_collection = None

    def _setup_collection(self, compute_graph):
        self._readings_collection = compute_graph.get_collection("readings")
        return self._readings_collection

    @computed
    def alerts(self) -> dict[str, str]:
        return self._readings_collection.temperature_alerts(self.threshold)


async def main():
    # Initialize service
    service = MetaService("temperature_monitor")

    # Register collections and resources
    service.register_collection(SensorReadingsCollection)
    service.register_resource(TemperatureMonitorResource)

    # Start service
    await service.start()

    # Start client
    client_task = asyncio.create_task(run_client())

    # Simulate sensor readings
    async def simulate_sensors():
        sensors = ["sensor1", "sensor2"]
        readings_collection = service.collections["readings"]
        while True:
            for sensor_id in sensors:
                reading = SensorReading(
                    sensor_id=sensor_id,
                    temperature=random.uniform(25.0, 35.0),
                    timestamp=datetime.now(),
                )
                readings_collection.set(sensor_id, reading)
                print("New reading - Sensor:", sensor_id, reading.temperature)
            await asyncio.sleep(2)

    # Start simulation
    simulation_task = asyncio.create_task(simulate_sensors())

    try:
        # Keep service running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        simulation_task.cancel()
        client_task.cancel()
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())

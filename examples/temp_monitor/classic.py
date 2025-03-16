import asyncio
from datetime import datetime
from typing import Dict, List, Tuple

from examples.temp_monitor.common import (
    SensorReading,
    evaluate_temperature_alert,
    LocationInfo,
    DEFAULT_LOCATIONS,
    run_temperature_simulation,
)
from src.reactive_framework.classic.mapper import (
    OneToOneMapper,
    ManyToOneMapper,
)
from src.reactive_framework.classic.resource import Resource, ResourceParams
from src.reactive_framework.classic.service import Service
from src.reactive_framework.core.compute_graph import ComputedCollection


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

        # Use common evaluation logic
        return evaluate_temperature_alert(
            avg_temp, location, location_info, self.global_threshold
        )


# Resource Parameters
class MonitorParams(ResourceParams):
    threshold: float  # Global alert threshold in Celsius


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

    # Populate location reference data with defaults
    for location, info in DEFAULT_LOCATIONS.items():
        location_references.set(location, info)

    # Create and add resource
    monitor = TemperatureMonitorResource(
        "temperature_monitor", readings, location_references, service.compute_graph
    )
    service.add_resource("monitor", monitor)

    # Define reading updater function for the simulation
    async def update_reading(sensor_id: str, location: str, temp: float):
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

    # Define location updater function
    async def update_location(location: str, info: LocationInfo):
        location_references.set(location, info)

    service_task, simulation_task = None, None

    try:
        # Create tasks for both the service and simulation
        service_task = asyncio.create_task(service.start())
        simulation_task = asyncio.create_task(
            run_temperature_simulation(update_reading, update_location)
        )

        # Wait for both tasks
        await asyncio.gather(service_task, simulation_task)
    except KeyboardInterrupt:
        if simulation_task:
            simulation_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())

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
from src.reactive_framework.core.compute_graph import ComputedCollection
from src.reactive_framework.meta import (
    Service,
    resource,
    one_to_one,
    many_to_one,
    map_collection,
)


# Define mappers using decorators
@many_to_one
def average_temperature(readings: List[SensorReading]) -> Tuple[float, str]:
    """Computes average temperature for each sensor from its readings"""

    if not readings:
        return 0.0, ""
    avg_temp = sum(r.temperature for r in readings) / len(readings)
    # Return both temperature and location
    return avg_temp, readings[0].location


@one_to_one
def enhanced_alert(value: Tuple[float, str], global_threshold: float) -> Dict[str, str]:
    """
    Generates enhanced alerts by comparing current temperatures
    with reference data for each location
    """
    avg_temp, location = value

    # Get reference data for this location
    location_info = location_references.get(location)

    # Use common evaluation logic
    return evaluate_temperature_alert(
        avg_temp, location, location_info, global_threshold
    )


# Define resource using decorator
@resource
def temperature_monitor(threshold: float):
    # Compute average temperatures
    averages = map_collection(readings, average_temperature)

    # Generate enhanced alerts using both averages and location references
    alerts = map_collection(averages, enhanced_alert, threshold)

    return alerts


# Create a service
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
for loc, info in DEFAULT_LOCATIONS.items():
    location_references.set(loc, info)

# Register the resource with the service
service.add_resource("monitor", temperature_monitor)


async def main():
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

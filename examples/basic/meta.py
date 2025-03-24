import asyncio
import time

from pydantic import BaseModel
from reactive.core.compute_graph import ComputedCollection
from reactive.meta import one_to_one, resource, Service, map_collection


# Define data models
class DataPoint(BaseModel):
    value: float
    timestamp: str


# Define a mapper with decorator
@one_to_one
def multiply_value(value, multiplier: float):
    if value is None:
        return None
    return {"value": value.value * multiplier, "timestamp": value.timestamp}


# Define a resource with decorator
@resource
def processor(multiplier: float):
    # The data collection is automatically detected as a dependency
    multiplied_data = map_collection(raw_data, multiply_value, multiplier)
    return multiplied_data


# Create a service
service = Service("data_processor", port=1234)

# Create a collection for raw data
raw_data = ComputedCollection("raw_data", service.compute_graph)


async def main():
    async def generate_data():
        val = 1
        while True:
            val += 1
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            raw_data.set(
                "sensor1",
                DataPoint(value=val, timestamp=timestamp),
            )
            print(f"sensor1 value: {val} time: {timestamp}")
            await asyncio.sleep(1)

    service_task = asyncio.create_task(service.start())
    simulation_task = asyncio.create_task(generate_data())
    await asyncio.gather(service_task, simulation_task)


if __name__ == "__main__":
    asyncio.run(main())

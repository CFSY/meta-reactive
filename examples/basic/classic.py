import asyncio
import time

from pydantic import BaseModel
from reactive.classic.mapper import OneToOneMapper
from reactive.classic.resource import Resource, ResourceParams
from reactive.classic.service import Service
from reactive.core.compute_graph import ComputedCollection


# Define data models
class DataPoint(BaseModel):
    value: float
    timestamp: str


# Define a mapper
class MultiValueMapper(OneToOneMapper):
    def __init__(self, multiplier: float):
        self.multiplier = multiplier

    def map_value(self, value):
        if value is None:
            return None
        return {"value": value.value * self.multiplier, "timestamp": value.timestamp}


# Define resource parameters
class ProcessorParams(ResourceParams):
    multiplier: float


# Define a resource
class DataProcessorResource(Resource):
    def __init__(self, data_collection, compute_graph):
        super().__init__(ProcessorParams, compute_graph)
        self.data = data_collection

    def setup_resource_collection(self, params):
        # Create a derived collection by mapping the input data
        multiplied_data = self.data.map(MultiValueMapper, params.multiplier)
        return multiplied_data


async def main():
    # Create a service
    service = Service("data_processor", port=1234)

    # Create a collection for raw data
    raw_data = ComputedCollection("raw_data", service.compute_graph)

    # Create and register our resource
    processor = DataProcessorResource(raw_data, service.compute_graph)
    service.add_resource("processor", processor)

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

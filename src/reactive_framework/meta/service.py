from typing import Dict

from .detector import FrameworkDetector
from .resource import Resource
from ..classic.service import Service as ClassicService
from ..core.compute_graph import ComputedCollection

# Create a framework detector for the meta API
detector = FrameworkDetector("reactive_meta")
FrameworkClass = detector.get_metaclass()


class Service(metaclass=FrameworkClass):
    """
    Service class for the metaprogramming API.
    This class serves as a wrapper around the classic Service class.
    """

    def __init__(self, name: str, host: str = "localhost", port: int = 8080):
        self.name = name
        self.host = host
        self.port = port
        self._classic_service = ClassicService(name, host, port)
        self._resources: Dict[str, Resource] = {}

    def add_resource(self, name: str, resource: Resource) -> None:
        """
        Add a resource to the service.

        Args:
            name: The name of the resource
            resource: The resource instance
        """
        # Initialize the classic resource with our compute graph
        classic_resource = resource._create_classic_resource(
            self._classic_service.compute_graph
        )

        # Add the resource to the classic service
        self._classic_service.add_resource(name, classic_resource)

        # Keep track of the resource
        self._resources[name] = resource

    def register_collection(self, collection: ComputedCollection) -> None:
        """
        Register a collection with the service.

        Args:
            collection: The collection to register
        """
        self._classic_service.collections[collection.name] = collection

    async def start(self) -> None:
        """Start the service"""
        await self._classic_service.start()

    @property
    def compute_graph(self):
        """Get the compute graph from the classic service"""
        return self._classic_service.compute_graph

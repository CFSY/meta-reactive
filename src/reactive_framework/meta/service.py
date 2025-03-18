from typing import Dict

from .common import FrameworkClass
from .resource import Resource
from ..classic.service import Service as ClassicService


class Service(metaclass=FrameworkClass):
    """
    Service class for the metaprogramming API.
    This class serves as a wrapper around the classic Service class.
    """

    def __init__(self, name: str, host: str = "localhost", port: int = 8080):
        self.host = host
        self.port = port
        self._classic_service = ClassicService(name, host, port)
        self._resources: Dict[str, Resource] = {}

    # TODO: This should be handled internally by the framework and not by the user
    def add_resource(self, name: str, resource: Resource) -> None:
        """
        Add a resource to the service.

        Args:
            name: The name of the resource
            resource: The resource instance
        """
        # Initialize the classic resource with our compute graph
        classic_resource = resource.create_classic_resource(
            self._classic_service.compute_graph
        )

        # Add the resource to the classic service
        self._classic_service.add_resource(name, classic_resource)

        # Keep track of the resource
        self._resources[name] = resource

    async def start(self) -> None:
        """Start the service"""
        await self._classic_service.start()

    @property
    def compute_graph(self):
        """Get the compute graph from the classic service"""
        return self._classic_service.compute_graph

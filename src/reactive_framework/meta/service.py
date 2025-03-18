from .common import FrameworkClass
from .resource import Resource, global_resource_registry
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

    def _add_resource(self, name: str, resource: Resource) -> None:
        # Initialize the classic resource with our compute graph
        classic_resource = resource.create_classic_resource(
            self._classic_service.compute_graph
        )

        # Add the resource to the classic service
        self._classic_service.add_resource(name, classic_resource)

    async def start(self) -> None:
        """Start the service"""

        # Register the resources on startup
        for resource_name, resource in global_resource_registry.items():
            self._add_resource(resource_name, resource)

        await self._classic_service.start()

    @property
    def compute_graph(self):
        """Get the compute graph from the classic service"""
        return self._classic_service.compute_graph

from typing import Generic, Dict, Any

from pydantic import BaseModel

from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.types import K, V


class ResourceParams(BaseModel):
    """Base class for resource parameters"""

    pass


class Resource(Generic[K, V]):
    def __init__(
        self, name: str, param_model: type[ResourceParams], compute_graph: ComputeGraph
    ):
        self.name = name
        self.param_model = param_model
        self.compute_graph = compute_graph

    def instantiate(self, params: Dict[str, Any]) -> Collection[K, V]:
        # Validate parameters
        validated_params = self.param_model.model_validate(params)

        # Create a new collection for this instance
        collection = self.create_collection(validated_params)

        # Set up dependencies
        self.setup_dependencies(collection, validated_params)

        return collection

    def create_collection(self, params: ResourceParams) -> Collection[K, V]:
        raise NotImplementedError

    def setup_dependencies(
        self, collection: Collection[K, V], params: ResourceParams
    ) -> None:
        raise NotImplementedError

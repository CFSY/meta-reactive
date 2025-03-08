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

    # Think of this as a collection factory configured by params
    def instantiate(self, params: Dict[str, Any]) -> Collection[K, V]:
        print("INSTANTIATING RESOURCE:", self.name)
        # Validate parameters
        validated_params = self.param_model.model_validate(params)

        # Create a new collection for this instance
        collection = self.create_collection(validated_params)

        return collection

    def create_collection(self, params: ResourceParams) -> Collection[K, V]:
        raise NotImplementedError

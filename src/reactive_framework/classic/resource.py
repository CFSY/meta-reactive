from typing import Generic, Dict, Any

from pydantic import BaseModel

from ..core.compute_graph import ComputeGraph, ComputedCollection
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
    def instantiate(self, params: Dict[str, Any]) -> ComputedCollection[K, V]:
        print("INSTANTIATING RESOURCE:", self.name)
        # Validate parameters
        validated_params = self.param_model.model_validate(params)

        # Create a new collection for this instance
        collection = self.setup_resource_collection(validated_params)

        return collection

    def setup_resource_collection(self, params: ResourceParams) -> ComputedCollection[K, V]:
        """Override this in derived classes to set up the resource's computed collection"""
        raise NotImplementedError

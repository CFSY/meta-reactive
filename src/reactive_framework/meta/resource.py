import inspect
from typing import Generic, Dict, Any, Optional, Type

from pydantic import BaseModel, create_model

from .analysis import CodeAnalyzer
from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.types import K, V


class MetaResource(Generic[K, V]):
    """Base class for resources managed by the meta API"""

    def __init__(
        self,
        name: str,
        compute_graph: ComputeGraph,
        param_model: Optional[Type[BaseModel]] = None,
    ):
        self.name = name
        self._compute_graph = compute_graph
        self._param_model = param_model or self._create_param_model()
        self._collection: Optional[Collection] = None

    def _create_param_model(self) -> Type[BaseModel]:
        """Create a parameter model from __init__ annotations"""
        init_params = inspect.signature(self.__init__).parameters
        return create_model(
            f"{self.__class__.__name__}Params",
            **{
                name: (param.annotation, ...)
                for name, param in init_params.items()
                if name != "self" and param.annotation != inspect.Parameter.empty
            },
        )

    def instantiate(self, params: Dict[str, Any]) -> Collection[K, V]:
        """Instantiate the resource with the given parameters"""
        # Validate parameters
        validated_params = self._param_model.model_validate(params)

        # Create instance
        self._collection = self._create_collection(validated_params)

        # Set up dependencies
        self._setup_dependencies()

        return self._collection

    def _create_collection(self, params: BaseModel) -> Collection[K, V]:
        """Create a collection for this resource instance"""
        raise NotImplementedError

    def _setup_dependencies(self) -> None:
        """Set up dependencies based on code analysis"""
        deps = CodeAnalyzer.analyze_class(self.__class__)
        for method_deps in deps.values():
            for dep_name, dep_info in method_deps.items():
                if dep_name in self._compute_graph._collections:
                    dep_collection = self._compute_graph._collections[dep_name]
                    self._compute_graph.add_dependency(
                        self._collection, dep_collection  # type: ignore
                    )

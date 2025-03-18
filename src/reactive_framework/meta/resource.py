import inspect
from typing import Any, Callable, Generic, Optional, Type, get_type_hints

from pydantic import BaseModel, create_model

from .detector import FrameworkDetector
from ..classic.resource import Resource as ClassicResource, ResourceParams
from ..core.compute_graph import ComputedCollection
from ..core.types import K, V

# Create a framework detector for the meta API
detector = FrameworkDetector("reactive_meta")
framework_function = detector.get_function_decorator()
FrameworkClass = detector.get_metaclass()


class Resource(Generic[K, V], metaclass=FrameworkClass):
    """
    Base class for reactive resources using the metaprogramming API.
    This class serves as a wrapper around the classic Resource class.
    """

    def __init__(
        self,
        name: str,
        param_model: Optional[Type[BaseModel]] = None,
    ):
        self.name = name
        self.param_model = param_model or ResourceParams
        self._classic_resource = None
        self._setup_method = None

    def create_classic_resource(self, compute_graph):
        """Create the underlying classic resource when compute_graph is available"""
        if self._classic_resource is None and compute_graph is not None:
            self._classic_resource = ClassicResource(
                self.name, self.param_model, compute_graph
            )
            # Override the setup method to use our implementation
            self._classic_resource.setup_resource_collection = (
                self._setup_resource_collection
            )
        return self._classic_resource

    def _setup_resource_collection(
        self, params: ResourceParams
    ) -> ComputedCollection[K, V]:
        """Delegate to the user-defined setup method"""
        if self._setup_method:
            # Extract the parameter values from the params object
            param_dict = params.model_dump() if hasattr(params, "model_dump") else {}

            # Call the setup method with unpacked parameters
            return self._setup_method(**param_dict)
        raise NotImplementedError(
            "Resource setup method not defined. Use @resource decorator or override setup."
        )

    def setup(self, setup_method: Callable):
        """Register a setup method for this resource"""
        self._setup_method = setup_method
        return setup_method


@framework_function
def resource(name: Optional[str] = None, param_model: Optional[Type[BaseModel]] = None):
    """
    Decorator to create a reactive resource.

    Args:
        name: Optional name for the resource. If not provided, the function name will be used.
        param_model: Optional Pydantic model for resource parameters.

    Returns:
        A decorator function that creates a Resource instance.
    """

    def decorator(func):
        # Get the resource name from the function name if not provided
        resource_name = name or func.__name__

        # If no param_model is provided, try to create one from function parameters
        actual_param_model = param_model
        if actual_param_model is None:
            # Get function signature
            sig = inspect.signature(func)
            # Get type hints
            type_hints = get_type_hints(func)

            # Create fields for the model
            fields = {}
            for param_name, param in sig.parameters.items():
                if param_name == "self":
                    continue

                # Get the type annotation if available
                param_type = type_hints.get(param_name, Any)

                # Add field with default value if provided
                if param.default is not inspect.Parameter.empty:
                    fields[param_name] = (param_type, param.default)
                else:
                    fields[param_name] = (param_type, ...)

            # Create a Pydantic model for the parameters
            if fields:
                actual_param_model = create_model(
                    f"{resource_name.title()}_Params", **fields
                )
            else:
                actual_param_model = ResourceParams

        # Create a resource instance
        resource_instance = Resource(resource_name, actual_param_model)

        # Register the setup method
        resource_instance.setup(func)

        return resource_instance

    # If called directly with a function
    if callable(name):
        func = name
        name = None
        return decorator(func)

    return decorator

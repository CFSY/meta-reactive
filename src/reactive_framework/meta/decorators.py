import functools
import inspect
import types
from typing import Any, Type, Optional, Dict, Callable, TypeVar

from pydantic import BaseModel, create_model

from .analysis import CodeAnalyzer, DependencyGraphBuilder
from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph, ComputedCollection

KT = TypeVar("KT")
VT = TypeVar("VT", bound=BaseModel)


def collection(name: str, key_type: Type[KT], value_type: Type[VT]) -> Callable:
    def decorator(cls: Type) -> Type:
        if not isinstance(key_type, type):
            raise TypeError(f"key_type must be a type, got {key_type}")

        if not isinstance(value_type, type) or not issubclass(value_type, BaseModel):
            raise TypeError(f"value_type must be a Pydantic model, got {value_type}")

        # Analyze dependencies in methods
        deps = CodeAnalyzer.analyze_class(cls)

        # Create namespace for new class
        namespace = {
            **{k: v for k, v in cls.__dict__.items() if not k.startswith("__")},
            "_dependencies": deps,
            "_key_type": key_type,
            "_value_type": value_type,
            "_collection_name": name,
        }
        bases = (ComputedCollection[key_type, value_type],)

        def exec_body(ns: Dict) -> None:
            ns.update(namespace)

        return types.new_class(cls.__name__, bases, {"metaclass": type}, exec_body)

    return decorator


def resource(
    name: Optional[str] = None, param_model: Optional[Type[BaseModel]] = None
) -> Callable:
    def decorator(cls: Type) -> Type:
        # Create parameter model if not provided
        nonlocal param_model
        if param_model is None:
            init_params = inspect.signature(cls.__init__).parameters
            param_model = create_model(
                f"{cls.__name__}Params",
                **{
                    param_name: (param.annotation, ...)
                    for param_name, param in init_params.items()
                    if param_name != "self"
                    and param.annotation != inspect.Parameter.empty
                },
            )

        class ResourceWrapper:
            def __init__(self, compute_graph: ComputeGraph):
                self.compute_graph = compute_graph
                self.instances: Dict[str, Any] = {}

            async def __call__(self, **params) -> Collection:
                # Validate parameters
                validated_params = param_model.model_validate(params)  # type: ignore

                # Create instance
                instance = cls(**validated_params.model_dump())

                # Analyze dependencies
                deps = CodeAnalyzer.analyze_class(cls)

                # Set up compute graph
                collection = instance._setup_collection(self.compute_graph)

                # Store instance
                instance_id = id(instance)
                self.instances[instance_id] = instance

                return collection

        # Add metadata to wrapper
        ResourceWrapper._resource_name = name or cls.__name__.lower()
        ResourceWrapper._param_model = param_model

        return ResourceWrapper

    return decorator


def computed(func: Callable) -> property:
    @functools.wraps(func)
    def wrapper(self: Any) -> Any:
        # Get or create cache key
        cache_key = f"computed_{func.__name__}"

        # Check cache
        if hasattr(self, f"_{cache_key}"):
            return getattr(self, f"_{cache_key}")

        # Compute value
        result = func(self)

        # Cache result
        setattr(self, f"_{cache_key}", result)

        return result

    return property(wrapper)


def reactive(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        # Get dependencies
        deps = CodeAnalyzer.analyze_function(func)

        # Validate dependencies
        builder = DependencyGraphBuilder()
        builder.add_dependencies(func.__name__, deps)
        builder.validate()

        # Execute function
        result = func(self, *args, **kwargs)

        # Invalidate computed properties
        for attr in dir(self):
            if attr.startswith("_computed_"):
                delattr(self, attr)

        return result

    return wrapper

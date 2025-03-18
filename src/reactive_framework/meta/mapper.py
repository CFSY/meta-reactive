from functools import wraps
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    Set,
    TypeVar,
)

from .detector import FrameworkDetector
from ..classic.mapper import (
    Mapper as ClassicMapper,
    OneToOneMapper as ClassicOneToOneMapper,
    ManyToOneMapper as ClassicManyToOneMapper,
)
from ..core.compute_graph import ComputedCollection

# Create a framework detector for the meta API
detector = FrameworkDetector("reactive_meta")
framework_function = detector.get_function_decorator()

K1 = TypeVar("K1")
V1 = TypeVar("V1")
K2 = TypeVar("K2")
V2 = TypeVar("V2")


class MapperWrapper(Generic[K1, V1, K2, V2]):
    """
    Wrapper class for mappers that handles dependency detection and
    provides a more convenient interface.
    """

    def __init__(
        self,
        mapper_func: Callable,
        mapper_type: str,
    ):
        self.mapper_func = mapper_func
        self.mapper_type = mapper_type
        self.dependencies: Set[ComputedCollection] = set()

        # Detect framework references in the mapper function
        self._detect_dependencies()

    def _detect_dependencies(self):
        """Detect ComputedCollection dependencies in the mapper function"""
        refs = detector.get_framework_references(self.mapper_func)
        if not refs:
            return

        # Get the global namespace of the mapper function
        globals_dict = self.mapper_func.__globals__

        # Check each reference to see if it's a ComputedCollection
        for ref in refs:
            # Handle attribute access (obj.attr)
            if "." in ref:
                obj_name, attr_name = ref.split(".", 1)
                # Remove trailing () if it's a method call
                if attr_name.endswith("()"):
                    attr_name = attr_name[:-2]

                # Try to get the object from globals
                obj = globals_dict.get(obj_name)
                if obj is not None:
                    try:
                        attr = getattr(obj, attr_name)
                        if isinstance(attr, ComputedCollection):
                            self.dependencies.add(attr)
                    except (AttributeError, TypeError):
                        pass
            else:
                # Direct variable reference
                # Remove trailing () if it's a function call
                var_name = ref[:-2] if ref.endswith("()") else ref
                var = globals_dict.get(var_name)
                if isinstance(var, ComputedCollection):
                    self.dependencies.add(var)

    def create_mapper(self, *args, **kwargs) -> ClassicMapper:
        """Create an instance of the classic mapper with the detected dependencies"""
        # Determine the mapper class based on the type
        if self.mapper_type == "one_to_one":
            mapper_class = _OneToOneMapperImpl
        elif self.mapper_type == "many_to_one":
            mapper_class = _ManyToOneMapperImpl
        else:
            raise ValueError(f"Unknown mapper type: {self.mapper_type}")

        # Combine explicitly provided dependencies with detected ones
        all_args = list(args)

        # Add detected dependencies that weren't explicitly provided
        for dep in self.dependencies:
            if dep not in all_args:
                all_args.append(dep)

        # Create the mapper instance
        return mapper_class(self.mapper_func, *all_args, **kwargs)


@framework_function
def mapper(mapper_type: str = "one_to_one"):
    """
    Decorator to create a mapper function.

    Args:
        mapper_type: Type of mapper ("one_to_one" or "many_to_one")

    Returns:
        A decorator function that creates a MapperWrapper instance.
    """

    def decorator(func):
        wrapper = MapperWrapper(func, mapper_type)
        # Preserve the original function attributes
        wraps(func)(wrapper)
        return wrapper

    return decorator


@framework_function
def one_to_one(func):
    """
    Decorator to create a one-to-one mapper function.

    Args:
        func: The mapper function

    Returns:
        A MapperWrapper instance.
    """
    wrapper = MapperWrapper(func, "one_to_one")
    # Preserve the original function attributes
    wraps(func)(wrapper)
    return wrapper


@framework_function
def many_to_one(func):
    """
    Decorator to create a many-to-one mapper function.

    Args:
        func: The mapper function

    Returns:
        A MapperWrapper instance.
    """
    wrapper = MapperWrapper(func, "many_to_one")
    # Preserve the original function attributes
    wraps(func)(wrapper)
    return wrapper


class _OneToOneMapperImpl(ClassicOneToOneMapper):
    """Implementation of OneToOneMapper that uses a function"""

    def __init__(self, map_func: Callable, *args, **kwargs):
        self.map_func = map_func
        self.args = args
        self.kwargs = kwargs

    def map_value(self, value: V1) -> Optional[V2]:
        return self.map_func(value, *self.args, **self.kwargs)


class _ManyToOneMapperImpl(ClassicManyToOneMapper):
    """Implementation of ManyToOneMapper that uses a function"""

    def __init__(self, map_func: Callable, *args, **kwargs):
        self.map_func = map_func
        self.args = args
        self.kwargs = kwargs

    def map_values(self, values: List[V1]) -> Optional[V2]:
        return self.map_func(values, *self.args, **self.kwargs)


def map_collection(
    collection: ComputedCollection, mapper_wrapper: MapperWrapper, *args, **kwargs
) -> ComputedCollection:
    """
    Map a collection using a mapper wrapper.

    Args:
        collection: The collection to map
        mapper_wrapper: The mapper wrapper to use
        *args: Additional arguments to pass to the mapper
        **kwargs: Additional keyword arguments to pass to the mapper

    Returns:
        A new computed collection with the mapped data
    """
    # Determine the mapper class to use with the classic API
    if mapper_wrapper.mapper_type == "one_to_one":
        mapper_class = _OneToOneMapperImpl
    else:
        mapper_class = _ManyToOneMapperImpl

    # Use the classic map method with the correct mapper class and function
    return collection.map(
        mapper_class,  # Pass the class, not the instance
        mapper_wrapper.mapper_func,  # Pass the original function
        *args,
        **kwargs,
    )

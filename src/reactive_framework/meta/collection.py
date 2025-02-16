import inspect

from .analysis import CodeAnalyzer
from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.types import K, V, Change


class MetaCollection(Collection[K, V]):
    """A collection managed by the meta API"""
    def __init__(self, name: str, compute_graph: ComputeGraph):
        super().__init__(name)
        self._compute_graph = compute_graph
        self._compute_graph.add_node(self)
        self._setup_dependencies()

    def _setup_dependencies(self) -> None:
        """Automatically set up dependencies based on code analysis"""
        # Analyze all methods
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(method, '_is_computed') or hasattr(method, '_is_reactive'):
                deps = CodeAnalyzer.analyze_function(method)
                for dep_name, dep_info in deps.items():
                    if dep_name in self._compute_graph._collections:
                        dep_collection = self._compute_graph._collections[dep_name]
                        self._compute_graph.add_dependency(self, dep_collection)

    def _handle_change(self, source: Collection[K, V], change: Change[K, V]) -> None:
        """Clear computed properties when dependencies change"""
        for name, _ in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(getattr(self, name), '_is_computed'):
                cache_name = f"_{name}_cache"
                if hasattr(self, cache_name):
                    delattr(self, cache_name)
        super()._handle_change(source, change)

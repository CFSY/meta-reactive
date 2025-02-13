from typing import Dict, Optional

from ..core.collection import Collection
from ..core.compute_graph import ComputeGraph
from ..core.types import K, V, Change


class ManagedCollection(Collection[K, V]):
    """A collection that can be managed by the classic API"""
    def __init__(self, name: str, compute_graph: ComputeGraph):
        super().__init__(name)
        self._compute_graph = compute_graph
        self._compute_graph.add_node(self)

    def add_dependency(self, dependency: Collection[K, V]) -> None:
        """Add a dependency to this collection"""
        self._compute_graph.add_dependency(self, dependency)

    def remove_dependency(self, dependency: Collection[K, V]) -> None:
        """Remove a dependency from this collection"""
        self._compute_graph.remove_dependency(self, dependency)

class CachedCollection(ManagedCollection[K, V]):
    """A collection with caching capabilities"""
    def __init__(self, name: str, compute_graph: ComputeGraph):
        super().__init__(name, compute_graph)
        self._cache: Dict[str, V] = {}

    def get_cached(self, key: K, cache_key: str) -> Optional[V]:
        """Get a cached value"""
        return self._cache.get(f"{key}:{cache_key}")

    def set_cached(self, key: K, cache_key: str, value: V) -> None:
        """Set a cached value"""
        self._cache[f"{key}:{cache_key}"] = value

    def clear_cache(self) -> None:
        """Clear the cache"""
        self._cache.clear()

    def _handle_change(self, source: Collection[K, V], change: Change[K, V]) -> None:
        """Clear cache when dependencies change"""
        self.clear_cache()
        super()._handle_change(source, change)

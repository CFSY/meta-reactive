import hashlib
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, TypeVar, Callable

from .collection import Collection
from .types import K, V, DependencyNode, Change, ComputeResult

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ComputeGraph:
    def __init__(self):
        self._nodes: Dict[str, DependencyNode] = {}
        self._collections: Dict[str, Collection] = {}
        self._lock = threading.RLock()
        self._computation_in_progress: Set[str] = set()

    def add_node(self, collection: Collection) -> None:
        with self._lock:
            node_id = collection.name
            if node_id not in self._nodes:
                self._nodes[node_id] = DependencyNode(id=node_id)
                self._collections[node_id] = collection
            # TODO: add warning for repeated node id

    def add_dependency(self, dependent: Collection, dependency: Collection) -> None:
        with self._lock:
            dep_node = self._nodes[dependency.name]
            dependent_node = self._nodes[dependent.name]

            if dependent.name not in dep_node.dependents:
                dep_node.dependents.append(dependent.name)

            if dependency.name not in dependent_node.dependencies:
                dependent_node.dependencies.append(dependency.name)
                dependency.add_observer(dependent)

    def remove_dependency(self, dependent: Collection, dependency: Collection) -> None:
        with self._lock:
            dep_node = self._nodes[dependency.name]
            dependent_node = self._nodes[dependent.name]

            if dependent.name in dep_node.dependents:
                dep_node.dependents.remove(dependent.name)

            if dependency.name in dependent_node.dependencies:
                dependent_node.dependencies.remove(dependency.name)
                dependency.remove_observer(dependent)

    def invalidate_node(self, node_id: str) -> None:
        with self._lock:
            node = self._nodes[node_id]
            if not node.invalidated:
                print("GRAPH INVALIDATING:", node.id)
                node.invalidated = True
                # Recursively invalidate all dependent nodes
                for dependent_id in node.dependents:
                    self.invalidate_node(dependent_id)

    def compute_node(self, node_id: str, force: bool = False) -> None:
        with self._lock:
            if node_id in self._computation_in_progress:
                logger.warning(f"Circular dependency detected for node {node_id}")
                return

            node = self._nodes[node_id]
            collection = self._collections[node_id]

            if not node.invalidated and not force:
                print("SKIP COMPUTE:", node.id)
                return

            self._computation_in_progress.add(node_id)
            try:
                # First compute all dependencies
                for dep_id in node.dependencies:
                    print("CALL COMPUTE (DEP):", node.id, "=>", dep_id)
                    self.compute_node(dep_id, force)

                # Now compute this node
                if hasattr(collection, "_compute"):
                    collection._compute()  # type: ignore

                node.invalidated = False
                node.last_computed = datetime.now()
            finally:
                self._computation_in_progress.remove(node_id)

    def get_node_status(self, node_id: str) -> DependencyNode:
        with self._lock:
            return self._nodes[node_id]

    def get_collection(self, node_id: str) -> Collection:
        with self._lock:
            return self._collections[node_id]


class ComputedCollection(Collection[K, V]):
    def __init__(self, name: str, compute_graph: ComputeGraph):
        super().__init__(name)
        self._compute_graph = compute_graph
        self._compute_graph.add_node(self)
        self._compute_func: Optional[Callable[[], Dict[K, V]]] = None
        self._cache_timeout = timedelta(seconds=60)

    def set_compute_func(self, func: Callable[[], Dict[K, V]]) -> None:
        self._compute_func = func

    def _compute(self) -> None:
        if self._compute_func is None:
            return

        print("ACTUAL COMPUTE:", self.name)

        # Generate cache key based on dependencies' last modified times
        cache_key = self._generate_cache_key()
        cached_result = self.get_cached_result(cache_key)

        if (
            cached_result is not None
            and datetime.now() - cached_result.computed_at < self._cache_timeout
        ):
            # Use cached result
            for change in cached_result.changes:
                if change.new_value is None:
                    self._data.pop(change.key, None)
                else:
                    self._data[change.key] = change.new_value
            return

        # Compute new values
        new_data = self._compute_func()
        changes: list[Change[K, V]] = []

        # Calculate changes
        old_keys = set(self._data.keys())
        new_keys = set(new_data.keys())

        # Handle deletions
        for key in old_keys - new_keys:
            changes.append(Change(key=key, old_value=self._data[key], new_value=None))
            del self._data[key]

        # Handle updates and additions
        for key in new_keys:
            old_value = self._data.get(key)
            new_value = new_data[key]
            if old_value != new_value:
                changes.append(
                    Change(key=key, old_value=old_value, new_value=new_value)
                )
                self._data[key] = new_value

        # Cache the result
        self.set_cached_result(
            cache_key, ComputeResult(changes=changes, cache_key=cache_key)
        )

        # Notify observers of changes
        for change in changes:
            print("NOTIFY COMPUTED CHANGES:", change)
            self._notify_observers(change)

    def _generate_cache_key(self) -> str:
        node = self._compute_graph.get_node_status(self.name)
        components = []
        for dep_id in node.dependencies:
            dep_collection = self._compute_graph.get_collection(dep_id)
            components.append(f"{dep_id}:{dep_collection._last_modified.timestamp()}")

        key_string = "|".join(components)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def _handle_change(self, source: Collection[K, V], change: Change[K, V]) -> None:
        print("HANDLE CHANGE", self.name, change.old_value, "=>", change.new_value)
        self._compute_graph.invalidate_node(self.name)
        self._compute_graph.compute_node(self.name)

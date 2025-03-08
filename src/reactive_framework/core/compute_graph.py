import hashlib
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, TypeVar, Callable, List, Tuple

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
        self._coordinated_update_in_progress: bool = False

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

    def invalidate_node(self, node_id: str) -> Set[str]:
        """
        Invalidates a node and all its dependents.
        Returns the set of all invalidated nodes.
        """
        with self._lock:
            return self._invalidate_node_recursive(node_id, set())

    def _invalidate_node_recursive(
        self, node_id: str, invalidated: Set[str]
    ) -> Set[str]:
        """
        Recursive helper for invalidate_node that tracks all invalidated nodes.
        """
        node = self._nodes[node_id]
        if not node.invalidated:
            print("GRAPH INVALIDATING:", node.id)
            node.invalidated = True
            invalidated.add(node_id)

            # Recursively invalidate all dependent nodes
            for dependent_id in node.dependents:
                self._invalidate_node_recursive(dependent_id, invalidated)

        return invalidated

    def recompute_invalidated(self, starting_node_id: str) -> None:
        """
        Recomputes all invalidated nodes starting from a specific node.
        Uses a proper topological ordering to minimize recomputation.
        """
        with self._lock:
            # Avoid nested coordinated updates
            if self._coordinated_update_in_progress:
                return

            try:
                self._coordinated_update_in_progress = True

                # First, invalidate the node and all its dependents
                invalidated_nodes = self.invalidate_node(starting_node_id)

                # Get a topologically sorted list of invalidated nodes to compute
                # We need to compute in reverse dependency order (dependencies before dependents)
                sorted_nodes = self._topological_sort(invalidated_nodes)

                # Collect all changes during computation to notify later
                all_changes: Dict[str, List[Tuple[Collection, Change]]] = {}

                # Compute each node in order
                for node_id in sorted_nodes:
                    if self._nodes[node_id].invalidated:
                        changes = self._compute_single_node(
                            node_id, suppress_notifications=True
                        )
                        if changes:
                            all_changes[node_id] = changes

                # Now process all the notifications in the correct order
                for node_id in sorted_nodes:
                    if node_id in all_changes:
                        for collection, change in all_changes[node_id]:
                            # We're only notifying change callbacks here, not triggering observer updates
                            # since those have already been handled in our topological computation
                            collection._notify_callbacks_only(change)
            finally:
                self._coordinated_update_in_progress = False

    def _topological_sort(self, node_ids: Set[str]) -> List[str]:
        """
        Returns a topologically sorted list of the nodes that need to be computed.
        Dependencies come before dependents.
        """
        result = []
        visited = set()
        temp_mark = set()

        def visit(node_id: str) -> None:
            if node_id in temp_mark:
                logger.warning(f"Circular dependency detected involving node {node_id}")
                return
            if node_id not in visited and node_id in self._nodes:
                temp_mark.add(node_id)
                node = self._nodes[node_id]
                for dep_id in node.dependencies:
                    visit(dep_id)
                temp_mark.remove(node_id)
                visited.add(node_id)
                result.append(node_id)

        # Visit all nodes in the invalidated set
        for node_id in node_ids:
            if node_id not in visited:
                visit(node_id)

        return result

    def _compute_single_node(
        self, node_id: str, suppress_notifications: bool = False
    ) -> Optional[List[Tuple[Collection, Change]]]:
        """
        Computes a single node without recursion.
        Assumes all dependencies have already been computed.
        Returns a list of (collection, change) tuples if suppress_notifications is True
        """
        node = self._nodes[node_id]
        collection = self._collections[node_id]

        # Skip if this node is somehow involved in an in-progress computation
        if node_id in self._computation_in_progress:
            logger.warning(f"Circular dependency detected for node {node_id}")
            return None

        self._computation_in_progress.add(node_id)
        try:
            # Compute this node
            print("COMPUTING NODE:", node_id)
            changes = collection._compute(suppress_notifications=suppress_notifications)  # type: ignore

            node.invalidated = False
            node.last_computed = datetime.now()
            return changes
        finally:
            self._computation_in_progress.remove(node_id)

    def compute_node(self, node_id: str, force: bool = False) -> None:
        """
        Legacy method to maintain compatibility.
        Now just delegates to recompute_invalidated.
        """
        if force or self._nodes[node_id].invalidated:
            self.recompute_invalidated(node_id)

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

    def _compute(
        self, suppress_notifications: bool = False
    ) -> Optional[List[Tuple["Collection", Change]]]:
        if self._compute_func is None:
            return None

        print("ACTUAL COMPUTE:", self.name)

        # Generate cache key based on dependencies' last modified times
        cache_key = self._generate_cache_key()
        cached_result = self.get_cached_result(cache_key)

        changes: list[Change[K, V]] = []

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
                changes.append(change)
        else:
            # Compute new values
            new_data = self._compute_func()

            # Calculate changes
            old_keys = set(self._data.keys())
            new_keys = set(new_data.keys())

            # Handle deletions
            for key in old_keys - new_keys:
                changes.append(
                    Change(key=key, old_value=self._data[key], new_value=None)
                )
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

        # During coordinated updates, we collect the changes instead of notifying immediately
        if suppress_notifications:
            return [(self, change) for change in changes]
        else:
            # In standalone mode, notify observers of changes normally
            for change in changes:
                print("NOTIFY COMPUTED CHANGES:", change)
                self._notify_observers(change)
            return None

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
        # Instead of immediately invalidating and computing in separate steps,
        # use the coordinated recomputation approach
        self._compute_graph.recompute_invalidated(self.name)

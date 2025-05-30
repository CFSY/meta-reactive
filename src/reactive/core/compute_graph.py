import hashlib
import logging
import threading
from datetime import datetime
from typing import Dict, Set, Optional, TypeVar, Callable, List, Tuple, Type, Any

from .collection import Collection
from .types import K, V, DependencyNode, Change

logger = logging.getLogger(__name__)

T = TypeVar("T")
K2 = TypeVar("K2")
V2 = TypeVar("V2")


class ComputeGraph:
    def __init__(self):
        self._nodes: Dict[str, DependencyNode] = {}
        self._collections: Dict[str, ComputedCollection] = {}
        self._lock = threading.RLock()
        self._computation_in_progress: Set[str] = set()
        self._coordinated_update_in_progress: bool = False

    def add_node(self, collection: "ComputedCollection") -> DependencyNode:
        with self._lock:
            node_id = collection.name
            # Ignore repeated node
            if node_id not in self._nodes:
                self._nodes[node_id] = DependencyNode(id=node_id)
                self._collections[node_id] = collection
            return self._nodes[node_id]

    def add_dependency(
        self, dependent: "ComputedCollection", dependency: "ComputedCollection"
    ) -> None:
        with self._lock:
            dep_node = self._nodes[dependency.name]
            dependent_node = self._nodes[dependent.name]

            if dependent.name not in dep_node.dependents:
                dep_node.dependents.append(dependent.name)

            if dependency.name not in dependent_node.dependencies:
                dependent_node.dependencies.append(dependency.name)

    def remove_dependency(
        self, dependent: "ComputedCollection", dependency: "ComputedCollection"
    ) -> None:
        with self._lock:
            dep_node = self._nodes[dependency.name]
            dependent_node = self._nodes[dependent.name]

            if dependent.name in dep_node.dependents:
                dep_node.dependents.remove(dependent.name)

            if dependency.name in dependent_node.dependencies:
                dependent_node.dependencies.remove(dependency.name)

    def invalidate_node(self, node_id: str) -> Set[str]:
        """
        Invalidates a node and all its dependents.
        Returns the set of all invalidated nodes.
        """
        with self._lock:
            invalidated = set()

            def _invalidate_recursive(current_node_id: str) -> None:
                node = self._nodes[current_node_id]
                if not node.invalidated:
                    node.invalidated = True
                    invalidated.add(current_node_id)

                    # Recursively invalidate all dependent nodes
                    for dependent_id in node.dependents:
                        _invalidate_recursive(dependent_id)

            _invalidate_recursive(node_id)
            return invalidated

    def recompute_invalidated(self, starting_node_id: str) -> None:
        """
        Recomputes all invalidated nodes starting from a specific node.
        1. Recursively invalidate all dependencies
        2. Perform re-computation in a topological ordering
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
                sorted_nodes = self._topological_sort(invalidated_nodes)

                # Collect all changes during computation to notify later
                all_changes: List[Tuple[str, List[Change]]] = []

                # Compute each node in order
                for node_id in sorted_nodes:
                    if self._nodes[node_id].invalidated:
                        changes = self._compute_single_node(node_id)
                        if changes:
                            all_changes.append((node_id, changes))

                # Now process all the notifications in the correct order
                for node_id, changes in all_changes:
                    for change in changes:
                        self._collections[node_id].notify_callbacks(change)

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

    def _compute_single_node(self, node_id: str) -> Optional[List[Change]]:
        """
        Computes a single node without recursion.
        Returns a list of changes
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
            changes = collection.compute()

            node.invalidated = False
            node.last_computed = datetime.now()

            return changes
        finally:
            self._computation_in_progress.remove(node_id)

    def get_node_status(self, node_id: str) -> DependencyNode:
        with self._lock:
            return self._nodes[node_id]

    def get_collection(self, node_id: str) -> "ComputedCollection":
        with self._lock:
            return self._collections[node_id]


class ComputedCollection(Collection[K, V]):
    def __init__(self, name: str, compute_graph: ComputeGraph):
        super().__init__(name)
        # attach itself to the compute graph
        self._compute_graph = compute_graph
        self._dependency_node = self._compute_graph.add_node(self)

        self._compute_func: Optional[Callable[[], Dict[K, V]]] = None

    def add_change_callback(
        self, instance_id: str, callback: Callable[[Change[K, V]], None]
    ) -> None:
        self._dependency_node.change_callbacks[instance_id] = callback

    def remove_callback(self, instance_id: str) -> None:
        if instance_id in self._dependency_node.change_callbacks:
            del self._dependency_node.change_callbacks[instance_id]

    def notify_callbacks(self, change: Change[K, V]) -> None:
        for callback in self._dependency_node.change_callbacks.values():
            callback(change)

    def handle_change(self, change: Change[K, V]) -> None:
        # Start coordinated re-computation
        self._compute_graph.recompute_invalidated(self.name)

    def set_compute_func(self, func: Callable[[], Dict[K, V]]) -> None:
        self._compute_func = func

    def compute(self) -> Optional[List[Change]]:
        if self._compute_func is None:
            return None

        changes: list[Change[K, V]] = []

        # Compute new values
        new_data = self._compute_func()

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

        return changes

    def map(
        self, mapper_class: Type, *args: Any, **kwargs: Any
    ) -> "ComputedCollection[K2, V2]":
        """
        Creates a new computed collection by applying a mapper to this collection.

        Args:
            mapper_class: The mapper class (not an instance) to use for the transformation.
            *args: Positional arguments to pass to the mapper constructor.
            **kwargs: Keyword arguments to pass to the mapper constructor.

        Returns:
            A new ComputedCollection containing the mapped data.
        """
        # Generate a unique name for the new collection based on the mapper class and its parameters.
        mapper_identifier = f"{mapper_class.__name__}:{args}:{kwargs}"
        hash_digest = hashlib.sha256(mapper_identifier.encode()).hexdigest()[:8]
        name = f"{self.name}_mapped_{hash_digest}"

        # Create the new computed collection
        result = ComputedCollection[K2, V2](name, self._compute_graph)

        # Add this collection as a dependency
        self._compute_graph.add_dependency(result, self)

        # Check for ComputedCollection dependencies in args and kwargs
        for arg in args:
            if isinstance(arg, ComputedCollection):
                self._compute_graph.add_dependency(result, arg)

        for arg in kwargs.values():
            if isinstance(arg, ComputedCollection):
                self._compute_graph.add_dependency(result, arg)

        # Instantiate the mapper with the provided arguments
        mapper = mapper_class(*args, **kwargs)

        # Define the compute function for the mapped collection
        def compute_func() -> Dict[K2, V2]:
            new_data: Dict[K2, V2] = {}
            for key, value in self.iter_items():
                for mapped_key, mapped_value in mapper.map_element(key, value):
                    new_data[mapped_key] = mapped_value
            return new_data

        # Set the compute function
        result.set_compute_func(compute_func)

        return result

import inspect
from dataclasses import dataclass
from typing import Dict, Set, Any, Optional

import libcst as cst
from libcst.metadata import PositionProvider


@dataclass
class DependencyInfo:
    collection_name: str
    accessed_fields: Set[str]
    is_write: bool = False


class DependencyAnalyzer(cst.CSTVisitor):
    METADATA_DEPENDENCIES = (PositionProvider,)

    def __init__(self):
        super().__init__()
        self.dependencies: Dict[str, DependencyInfo] = {}
        self._current_collection: Optional[str] = None

    def visit_Attribute(self, node: cst.Attribute) -> bool:
        # Check if this is a collection access
        if isinstance(node.value, cst.Name):
            collection_name = node.value.value
            if collection_name in self.dependencies:
                self.dependencies[collection_name].accessed_fields.add(node.attr.value)
        return True

    def visit_Call(self, node: cst.Call) -> bool:
        # Detect collection method calls
        if isinstance(node.func, cst.Attribute) and isinstance(
            node.func.value, cst.Name
        ):
            collection_name = node.func.value.value
            method_name = node.func.attr.value
            if collection_name in self.dependencies:
                if method_name in {"set", "delete", "update"}:
                    self.dependencies[collection_name].is_write = True
        return True


class CodeAnalyzer:
    @staticmethod
    def analyze_function(func: Any) -> Dict[str, DependencyInfo]:
        # Get function source code
        source = inspect.getsource(func)

        # Parse the source code
        module = cst.parse_module(source)

        # Create and run the analyzer
        analyzer = DependencyAnalyzer()
        module.visit(analyzer)

        return analyzer.dependencies

    @staticmethod
    def analyze_class(cls: type) -> Dict[str, Dict[str, DependencyInfo]]:
        results: Dict[str, Dict[str, DependencyInfo]] = {}

        # Analyze all methods in the class
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith("_"):  # Skip private methods
                results[name] = CodeAnalyzer.analyze_function(method)

        return results


class DependencyGraphBuilder:
    def __init__(self):
        self.dependencies: Dict[str, Set[str]] = {}
        self.writes: Dict[str, Set[str]] = {}

    def add_dependencies(self, source: str, deps: Dict[str, DependencyInfo]) -> None:
        if source not in self.dependencies:
            self.dependencies[source] = set()
            self.writes[source] = set()

        for dep_name, dep_info in deps.items():
            self.dependencies[source].add(dep_name)
            if dep_info.is_write:
                self.writes[source].add(dep_name)

    def validate(self) -> None:
        # Check for circular dependencies
        visited = set()
        path = []

        def dfs(node: str) -> None:
            if node in path:
                cycle = path[path.index(node) :] + [node]
                raise ValueError(f"Circular dependency detected: {' -> '.join(cycle)}")

            if node in visited:
                return

            visited.add(node)
            path.append(node)

            for dep in self.dependencies.get(node, set()):
                dfs(dep)

            path.pop()

        for node in self.dependencies:
            dfs(node)

        # Check for write conflicts
        all_writes = set()
        for writes in self.writes.values():
            conflicts = writes & all_writes
            if conflicts:
                raise ValueError(
                    f"Multiple collections attempting to write to: {conflicts}"
                )
            all_writes.update(writes)

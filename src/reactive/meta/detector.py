import functools
import inspect
import textwrap
from typing import Any, Callable, Dict, Optional, Set, Type, TypeVar, Union

import libcst as cst

T = TypeVar("T", bound=Callable)


class FrameworkDetector:
    """
    Provides utilities to mark functions/classes as framework components
    and detect usage of these components within Python code.

    Uses LibCST for static analysis to find references. Tracks components
    using special attributes based on the framework_name.
    """

    def __init__(self, framework_name: str = "generic_framework"):
        """
        Initializes the framework detector.

        Args:
            framework_name: A valid Python identifier used to namespace
                            internal attributes that mark components and store references.

        Raises:
            ValueError: If framework_name is not a valid Python identifier.
        """
        if not framework_name.isidentifier():
            raise ValueError("framework_name must be a valid Python identifier")
        self.framework_name = framework_name
        self.framework_attr = f"__{framework_name}_component__"
        self.framework_refs_attr = f"__{framework_name}_refs__"

        self._analyzer = CodeAnalyzer(self)
        # Cache analysis results to avoid re-computation and recursion loops
        self._analysis_cache: Dict[int, Set[str]] = {}

    def get_function_decorator(self) -> Callable[[T], T]:
        """
        Creates a decorator to mark a function as a framework component.

        The decorator also analyzes the function's source code upon definition
        to detect usage of other framework components and stores the results.

        Returns:
            A decorator function.
        """

        def decorator(func: T) -> T:
            if not callable(func):
                raise TypeError(
                    f"Decorator can only be applied to callable objects, got {type(func)}"
                )

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            # Mark the wrapper as a component
            setattr(wrapper, self.framework_attr, "function")

            # Analyze the original function and store references on the wrapper
            references = self.detect_framework_usage(func)
            setattr(wrapper, self.framework_refs_attr, references)

            return wrapper

        return decorator

    def get_metaclass(self) -> Type:
        """
        Creates a metaclass to mark a class as a framework component.

        The metaclass also analyzes the class's __init__ method and any
        user-defined methods upon class creation to detect usage of other
        framework components, storing the results on the class and methods.

        Returns:
            A metaclass for framework components.
        """
        detector_ref = self

        class ComponentMeta(type):
            def __new__(mcs, name: str, bases: tuple, attrs: dict) -> Type:
                new_class = super().__new__(mcs, name, bases, attrs)

                # Mark the class itself as a component
                setattr(new_class, detector_ref.framework_attr, "class")

                # Analyze __init__ for framework references
                init_refs = detector_ref.detect_framework_usage(new_class)
                if init_refs:
                    setattr(new_class, detector_ref.framework_refs_attr, init_refs)

                # Analyze user-defined methods defined directly in this class
                for attr_name, attr_value in attrs.items():
                    if inspect.isfunction(attr_value) and not attr_name.startswith(
                        "__"
                    ):
                        curr_method = getattr(new_class, attr_name)

                        # Mark the method as a component
                        setattr(curr_method, detector_ref.framework_attr, "method")

                        # Analyze the method for framework references
                        method_refs = detector_ref.detect_framework_usage(curr_method)
                        if method_refs:
                            setattr(
                                curr_method,
                                detector_ref.framework_refs_attr,
                                method_refs,
                            )

                return new_class

        return ComponentMeta

    def is_framework_component(self, obj: Any) -> bool:
        """
        Checks if an object has been marked as a framework component by this detector.

        Args:
            obj: The object to check.

        Returns:
            True if the object is marked as a component, False otherwise.
        """
        return hasattr(obj, self.framework_attr)

    def get_framework_references(self, obj: Any) -> Optional[Set[str]]:
        """
        Retrieves the stored set of framework references for a component.

        Args:
            obj: The framework component (function, class, or method) to check.

        Returns:
            A set of strings representing the detected framework references,
            or None if the object hasn't been analyzed or has no references stored.
        """
        return getattr(obj, self.framework_refs_attr, None)

    def detect_framework_usage(self, obj: Union[Callable, Type]) -> Set[str]:
        """
        Analyzes an object (function, method, or class) to detect usage of framework components.

        For classes, it specifically analyzes the `__init__` method.
        Uses caching to avoid redundant analysis and prevent infinite recursion.

        Args:
            obj: The object to analyze.

        Returns:
            A set of strings representing the detected framework references.
            Returns an empty set if analysis fails or no references are found.
        """
        try:
            # Use the underlying function for methods to ensure consistent caching key
            cache_key = id(obj.__func__) if inspect.ismethod(obj) else id(obj)
        except AttributeError:
            cache_key = id(obj)  # Fallback for other callables or classes

        if cache_key in self._analysis_cache:
            return self._analysis_cache[cache_key]

        # Placeholder to prevent recursion during analysis
        self._analysis_cache[cache_key] = set()

        result: Set[str] = set()
        try:
            if inspect.isfunction(obj) or inspect.ismethod(obj):
                result = self._analyzer.analyze_function(obj)
            elif inspect.isclass(obj):
                # For classes, analysis focuses on the __init__ method
                result = self._analyzer.analyze_class_init(obj)
            else:
                result = set()  # Not a supported type for analysis

        except Exception:
            # Analysis failed, return empty set. Consider logging the error.
            result = set()
        finally:
            # Store the final result in the cache
            self._analysis_cache[cache_key] = result

        return result


class CodeAnalyzer:
    """Performs static code analysis using LibCST to find framework references."""

    def __init__(self, framework_detector: FrameworkDetector):
        """
        Initializes the code analyzer.

        Args:
            framework_detector: The FrameworkDetector instance used to identify
                                known framework components.
        """
        self.detector = framework_detector

    def analyze_source(
        self,
        source_code: str,
        global_ns: Dict[str, Any],
        local_ns: Optional[Dict[str, Any]] = None,
    ) -> Set[str]:
        """
        Analyzes a string containing Python source code for framework references.

        Args:
            source_code: The Python code to analyze.
            global_ns: The global namespace for resolving names found in the code.
            local_ns: The local namespace (e.g., closure) for resolving names.

        Returns:
            A set of strings representing detected framework references.
        """
        references: Set[str] = set()
        try:
            # Dedent source code before parsing to handle decorated functions correctly
            module = cst.parse_module(textwrap.dedent(source_code))
            visitor = FrameworkReferenceCollector(
                self.detector, global_ns, local_ns or {}
            )
            module.visit(visitor)
            references = visitor.framework_references
        except Exception:
            # Parsing or visiting failed. Consider logging the error.
            return set()
        return references

    def analyze_function(self, func: Callable) -> Set[str]:
        """
        Analyzes a function or method for framework component usage.

        This includes analyzing the function's own source code and recursively
        analyzing other functions called within it to find indirect references.

        Args:
            func: The function or method to analyze.

        Returns:
            A set of strings representing both direct and indirect framework references.
        """
        all_references: Set[str] = set()
        try:
            source = inspect.getsource(func)
            dedented_source = textwrap.dedent(source)

            # Resolve namespaces needed for analysis
            func_globals = getattr(func, "__globals__", {})
            base_func = func.__func__ if inspect.ismethod(func) else func
            module_globals = (
                inspect.getmodule(base_func).__dict__
                if inspect.getmodule(base_func)
                else {}
            )
            global_ns = {
                **module_globals,
                **func_globals,
            }  # Function's globals take precedence
            closure_ns = getattr(func, "__closure__", None)

            # 1. Analyze direct references in this function's source
            direct_references = self.analyze_source(
                dedented_source, global_ns, closure_ns or {}
            )
            all_references.update(direct_references)

            # 2. Find calls to other functions/methods within this source
            module = cst.parse_module(dedented_source)
            call_extractor = FunctionCallExtractor(global_ns, closure_ns or {})
            module.visit(call_extractor)

            # 3. Recursively analyze called functions for their references
            for called_func in call_extractor.called_functions:
                # Avoid infinite recursion for self-calls
                if called_func is func or (
                    inspect.ismethod(func) and called_func is func.__func__
                ):
                    continue

                try:
                    # Use the main detector entry point for analysis, leveraging caching
                    indirect_refs = self.detector.detect_framework_usage(called_func)
                    if indirect_refs:
                        all_references.update(indirect_refs)
                except Exception:
                    # Failed to analyze a called function, ignore and continue. Consider logging.
                    continue

        except (TypeError, OSError, IndentationError, Exception):
            # Failed to get source or parse. Consider logging.
            return set()

        # Remove potential duplicates (e.g., attribute access vs. method call)
        return self._deduplicate_references(all_references)

    def analyze_class_init(self, cls: Type) -> Set[str]:
        """
        Analyzes a class's __init__ method for framework component usage.

        Args:
            cls: The class whose __init__ method should be analyzed.

        Returns:
            A set of framework component references found in __init__.
        """
        try:
            init_method = getattr(cls, "__init__", None)
            # Ignore if no custom __init__ or if it's the basic object.__init__
            if not init_method or init_method is object.__init__:
                return set()

            # Analyze __init__ just like any other function/method
            # This leverages the caching and recursive analysis in detect_framework_usage
            return self.detector.detect_framework_usage(init_method)

        except Exception:
            # Analysis failed. Consider logging.
            return set()

    def _deduplicate_references(self, framework_refs: Set[str]) -> Set[str]:
        """
        Cleans up detected references.

        Specifically, if both an attribute access (`obj.attr`) and a method call
        on that attribute (`obj.attr()`) are detected, it prefers the method call
        representation.

        Args:
            framework_refs: The raw set of detected reference strings.

        Returns:
            A potentially smaller set with duplicates removed.
        """
        result = set()
        method_calls = {ref for ref in framework_refs if ref.endswith("()")}
        method_call_bases = {
            ref[:-2] for ref in method_calls
        }  # e.g., 'obj.method' from 'obj.method()'

        for ref in framework_refs:
            if ref.endswith("()"):
                # Always keep method calls
                result.add(ref)
            elif ref not in method_call_bases:
                # Keep attribute access only if no corresponding method call was found
                result.add(ref)
        return result


class FrameworkReferenceCollector(cst.CSTVisitor):
    """
    LibCST Visitor that traverses code and collects references to known
    framework components based on the provided detector and namespaces.
    """

    def __init__(
        self,
        framework_detector: FrameworkDetector,
        global_namespace: Dict[str, Any],
        local_namespace: Dict[str, Any],
    ):
        super().__init__()
        self.detector = framework_detector
        self.framework_references: Set[str] = set()
        # Combine namespaces for resolution, local scope takes precedence
        self.combined_namespace = {**global_namespace, **local_namespace}

    def _resolve_name(self, name: str) -> Optional[Any]:
        """Resolves a simple name using the combined local/global namespace."""
        return self.combined_namespace.get(name)

    def _get_full_attribute_path(self, node: cst.Attribute) -> Optional[str]:
        """Helper to reconstruct dotted paths like 'a.b.c' from CST nodes."""
        path_parts = []
        current_node = node
        while isinstance(current_node, cst.Attribute):
            path_parts.append(current_node.attr.value)
            current_node = current_node.value
        if isinstance(current_node, cst.Name):
            path_parts.append(current_node.value)
            return ".".join(reversed(path_parts))
        return None  # Path doesn't start with a simple name (e.g., call result)

    def visit_Call(self, node: cst.Call) -> None:
        """Visits function/method calls."""
        if isinstance(node.func, cst.Name):
            # Direct call like framework_func() or FrameworkClass()
            func_name = node.func.value
            resolved_obj = self._resolve_name(func_name)
            if resolved_obj and self.detector.is_framework_component(resolved_obj):
                self.framework_references.add(f"{func_name}()")

        elif isinstance(node.func, cst.Attribute):
            # Method call like obj.method() or Class.static_method()
            base_node = node.func.value
            method_name = node.func.attr.value

            if isinstance(base_node, cst.Name):
                obj_name = base_node.value
                base_obj = self._resolve_name(obj_name)
                if base_obj:
                    try:
                        target_attr = getattr(base_obj, method_name, None)
                        # Record if the method itself is marked as a component
                        if target_attr and self.detector.is_framework_component(
                            target_attr
                        ):
                            self.framework_references.add(f"{obj_name}.{method_name}()")
                        # Also record if the base object/class is marked (calling a regular method on a framework object)
                        elif self.detector.is_framework_component(base_obj):
                            self.framework_references.add(f"{obj_name}.{method_name}()")
                    except Exception:
                        pass  # Ignore getattr errors on unusual objects

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Visits attribute accesses like obj.attr."""
        # This might record attributes that are immediately called (e.g., `obj.method` part of `obj.method()`).
        # The _deduplicate_references method handles preferring the call `()` form later.
        base_node = node.value
        attr_name = node.attr.value

        if isinstance(base_node, cst.Name):
            obj_name = base_node.value
            base_obj = self._resolve_name(obj_name)
            if base_obj:
                try:
                    target_attr = getattr(base_obj, attr_name, None)
                    # Record if the attribute itself is marked (e.g., a nested component)
                    if target_attr and self.detector.is_framework_component(
                        target_attr
                    ):
                        self.framework_references.add(f"{obj_name}.{attr_name}")
                    # Also record if accessing an attribute on a marked object/class
                    elif self.detector.is_framework_component(base_obj):
                        self.framework_references.add(f"{obj_name}.{attr_name}")
                except Exception:
                    pass  # Ignore getattr errors


# Note: FunctionBodyExtractor is primarily used internally by analyze_class_init
# but is kept separate for clarity. It's less likely needed by end-users.
class FunctionBodyExtractor(cst.CSTVisitor):
    """LibCST Visitor that extracts the CST node for a specific function's body."""

    def __init__(self, target_function_name: str):
        super().__init__()
        self.target_function_name = target_function_name
        self.function_body: Optional[cst.BaseSuite] = None
        self.found = False

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        """Checks if the function definition matches the target name."""
        if node.name.value == self.target_function_name:
            self.function_body = node.body
            self.found = True
            return False  # Stop visiting deeper within this node or siblings
        return True  # Continue searching other definitions


class FunctionCallExtractor(cst.CSTVisitor):
    """
    LibCST Visitor that finds all functions/methods called within a scope
    and attempts to resolve them to actual callable objects.
    """

    def __init__(
        self,
        global_namespace: Dict[str, Any],
        local_namespace: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self.combined_namespace = {**global_namespace, **(local_namespace or {})}
        # Stores the actual callable objects found
        self.called_functions: Set[Callable] = set()

    def _resolve_name(self, name: str) -> Optional[Any]:
        """Resolves a simple name using the combined local/global namespace."""
        return self.combined_namespace.get(name)

    def visit_Call(self, node: cst.Call) -> None:
        """Visits call nodes and tries to resolve the called object."""
        resolved_callable = None
        if isinstance(node.func, cst.Name):
            # Direct call: some_function()
            func_name = node.func.value
            resolved_callable = self._resolve_name(func_name)

        elif isinstance(node.func, cst.Attribute):
            # Method/attribute call: obj.method()
            base_node = node.func.value
            method_name = node.func.attr.value
            if isinstance(base_node, cst.Name):
                obj_name = base_node.value
                base_obj = self._resolve_name(obj_name)
                if base_obj:
                    try:
                        resolved_callable = getattr(base_obj, method_name, None)
                    except Exception:
                        resolved_callable = None
            # Note: Does not currently resolve complex bases like `get_obj().method()`

        if callable(resolved_callable):
            try:
                # Add the resolved callable object to the set
                self.called_functions.add(resolved_callable)
            except TypeError:
                # Ignore unhashable callables if they occur
                pass

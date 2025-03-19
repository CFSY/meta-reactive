import functools
import inspect
import textwrap
from typing import Any, Callable, Dict, Optional, Set, Type, TypeVar, Union

import libcst as cst

T = TypeVar("T", bound=Callable)


class FrameworkDetector:
    """
    This class provides utilities to mark functions and classes as framework components
    and to detect the usage of framework components within code.
    """

    def __init__(self, framework_name: str = "generic_framework"):
        """
        Initialize the framework detector.

        Args:
            framework_name: Name of the framework, used in attribute names
        """
        self.framework_name = framework_name
        self.framework_attr = f"__{framework_name}_component__"
        self.framework_refs_attr = f"__{framework_name}_refs__"

        # Create analyzer
        self._analyzer = CodeAnalyzer(self)

        # Cache for analyzed functions to avoid recursive loops
        self._analysis_cache = {}

    def get_function_decorator(self) -> Callable[[T], T]:
        """
        Creates a decorator that both marks a function as a framework component
        and detects framework component usage within the function.

        Returns:
            A decorator function
        """

        def decorator(func: T) -> T:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            setattr(wrapper, self.framework_attr, "function")
            setattr(
                wrapper, self.framework_refs_attr, self.detect_framework_usage(func)
            )

            return wrapper

        return decorator

    def get_metaclass(self) -> Type:
        """
        Creates a metaclass that both marks classes as framework components
        and detects framework component usage within the class and its methods.

        Returns:
            A metaclass for framework components
        """
        # Create a reference to the detector that will be accessible in the metaclass
        detector_ref = self

        class ComponentMeta(type):
            def __new__(mcs, name: str, bases: tuple, attrs: dict) -> Type:
                # First create the class
                new_class = super().__new__(mcs, name, bases, attrs)

                # Mark the class as a framework component
                setattr(new_class, detector_ref.framework_attr, "class")

                # Detect framework usage in the class constructor
                init_refs = detector_ref.detect_framework_usage(new_class)
                if init_refs:
                    setattr(
                        new_class,
                        detector_ref.framework_refs_attr,
                        init_refs,
                    )

                # Find and analyze each method of the class
                for attr_name, attr_value in attrs.items():
                    if callable(attr_value) and not attr_name.startswith("__"):
                        curr_method = getattr(new_class, attr_name)
                        setattr(curr_method, detector_ref.framework_attr, "method")
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
        Check if an object is a framework component.

        Args:
            obj: The object to check

        Returns:
            True if the object is a framework component
        """
        return hasattr(obj, self.framework_attr)

    def get_framework_references(self, obj: Any) -> Optional[Set[str]]:
        """
        Get the framework references used by an object if available.

        Args:
            obj: The object to check

        Returns:
            Set of references or None if not available
        """
        if hasattr(obj, self.framework_refs_attr):
            return getattr(obj, self.framework_refs_attr)
        return None

    def detect_framework_usage(self, obj: Union[Callable, Type]) -> Set[str]:
        """
        Detect framework component usage in a function, class, or class method.

        Args:
            obj: The object to analyze (function, class, or method)

        Returns:
            Set of framework component references found
        """
        # Use cached results if available
        obj_id = id(obj)
        if obj_id in self._analysis_cache:
            return self._analysis_cache[obj_id]

        # Mark as being analyzed to prevent infinite recursion
        self._analysis_cache[obj_id] = set()

        if inspect.isfunction(obj) or inspect.ismethod(obj):
            result = self._analyzer.analyze_function(obj)
            self._analysis_cache[obj_id] = result
            return result
        elif inspect.isclass(obj):
            class_refs = set()

            # Analyze class initialization
            init_refs = self._analyzer.analyze_class_init(obj)
            class_refs.update(init_refs)

            self._analysis_cache[obj_id] = class_refs
            return class_refs
        else:
            self._analysis_cache[obj_id] = set()
            return set()


class CodeAnalyzer:
    """Analyzes code for framework component usage."""

    def __init__(self, framework_detector: FrameworkDetector):
        """
        Initialize the code analyzer.

        Args:
            framework_detector: The framework detector to use
        """
        self.detector = framework_detector

    def analyze_source(
        self,
        source_code: str,
        global_ns: Dict[str, Any],
        local_ns: Dict[str, Any] = None,
    ) -> Set[str]:
        """
        Analyze source code for framework component usage.

        Args:
            source_code: The source code to analyze
            global_ns: Global namespace for resolving names
            local_ns: Local namespace for resolving names

        Returns:
            Set of framework component references found
        """
        try:
            module = cst.parse_module(source_code)
            visitor = FrameworkReferenceCollector(self.detector, global_ns, local_ns)
            module.visit(visitor)
            return self._deduplicate_references(visitor.framework_references)
        except Exception:
            return set()

    def analyze_function(self, func: Callable) -> Set[str]:
        """
        Analyze a function for framework component usage.

        Args:
            func: The function to analyze

        Returns:
            Set of framework component references found
        """
        try:
            source = inspect.getsource(func)
            source = textwrap.dedent(source)
            module_globals = inspect.getmodule(func).__dict__
            func_globals = func.__globals__

            # Create a combined global namespace
            global_ns = {}
            global_ns.update(module_globals)
            global_ns.update(func_globals)

            references = self.analyze_source(source, global_ns, func.__closure__ or {})

            # Also check for indirect references through function calls
            module = cst.parse_module(source)
            call_extractor = FunctionCallExtractor(global_ns, func.__closure__ or {})
            module.visit(call_extractor)

            # Check each called function for framework references
            for called_func in call_extractor.called_functions:
                if (
                    called_func is not func
                ):  # Avoid recursion on self-referential functions
                    # Get references from the called function
                    called_refs = self.detector.get_framework_references(called_func)

                    # If no cached references, try to analyze it
                    if called_refs is None and callable(called_func):
                        # Recursively analyze the called function
                        called_refs = self.detector.detect_framework_usage(called_func)

                    # Add any found references
                    if called_refs:
                        references.update(called_refs)

            return references
        except Exception as e:
            return set()

    def analyze_class_init(self, cls: Type) -> Set[str]:
        """
        Analyze a class's __init__ method for framework component usage.

        Args:
            cls: The class to analyze

        Returns:
            Set of framework component references found
        """
        try:
            if not hasattr(cls, "__init__") or cls.__init__ is object.__init__:
                return set()

            source = inspect.getsource(cls)
            module = cst.parse_module(source)

            # Extract the __init__ method
            extractor = FunctionBodyExtractor("__init__")
            module.visit(extractor)

            if not extractor.found or not extractor.function_body:
                return set()

            # Create a module with just the __init__ body
            init_module = cst.Module(body=extractor.function_body.body)

            # Get the namespaces
            module_globals = inspect.getmodule(cls).__dict__

            # Analyze the init body
            visitor = FrameworkReferenceCollector(self.detector, module_globals)
            init_module.visit(visitor)

            dedup_references = self._deduplicate_references(
                visitor.framework_references
            )

            # Check for indirect function calls in __init__
            call_extractor = FunctionCallExtractor(module_globals)
            init_module.visit(call_extractor)

            for called_func in call_extractor.called_functions:
                # Get references from the called function
                called_refs = self.detector.get_framework_references(called_func)

                # If no cached references, try to analyze it
                if called_refs is None and callable(called_func):
                    called_refs = self.detector.detect_framework_usage(called_func)

                # Add any found references
                if called_refs:
                    dedup_references.update(called_refs)

            return dedup_references

        except Exception:
            return set()

    def analyze_class_method(self, cls: Type, method_name: str) -> Set[str]:
        """
        Analyze a specific class method for framework component usage.

        Args:
            cls: The class containing the method
            method_name: The name of the method to analyze

        Returns:
            Set of framework component references found
        """
        if not hasattr(cls, method_name):
            return set()

        method_ref = getattr(cls, method_name)
        if not callable(method_ref):
            return set()

        return self.analyze_function(method_ref)

    def _deduplicate_references(self, framework_refs: Set[str]) -> Set[str]:
        """
        Remove duplicate references where an attribute is also detected as a method call.

        Args:
            framework_refs: Set of detected references

        Returns:
            Deduplicated set of references
        """
        result = set()
        method_calls = {r for r in framework_refs if r.endswith("()")}

        for curr_ref in framework_refs:
            # If this is a method call, always include it
            if curr_ref.endswith("()"):
                result.add(curr_ref)
            else:
                # For an attribute access, check if there's also a method call version
                method_call_version = f"{curr_ref}()"
                if method_call_version not in method_calls:
                    result.add(curr_ref)

        return result


class FrameworkReferenceCollector(cst.CSTVisitor):
    """Visitor that collects references to framework components in code."""

    def __init__(
        self,
        framework_detector: FrameworkDetector,
        global_namespace: Dict[str, Any],
        local_namespace: Dict[str, Any] = None,
    ):
        super().__init__()
        self.detector = framework_detector
        self.framework_references: Set[str] = set()
        self.global_namespace = global_namespace
        self.local_namespace = local_namespace or {}

    def _resolve_name(self, name: str) -> Optional[Any]:
        """Try to resolve a name to its actual object."""
        if name in self.local_namespace:
            return self.local_namespace.get(name)
        return self.global_namespace.get(name)

    def visit_Call(self, node: cst.Call) -> None:
        """Detect calls to framework functions or constructors."""
        if isinstance(node.func, cst.Name):
            # Direct function call: framework_function()
            func_name = node.func.value
            func_obj = self._resolve_name(func_name)

            if func_obj and self.detector.is_framework_component(func_obj):
                self.framework_references.add(f"{func_name}()")

        elif isinstance(node.func, cst.Attribute):
            # Method call: obj.method()
            if isinstance(node.func.value, cst.Name):
                obj_name = node.func.value.value
                method_name = node.func.attr.value
                obj = self._resolve_name(obj_name)

                if obj and self.detector.is_framework_component(obj):
                    self.framework_references.add(f"{obj_name}.{method_name}()")

                # Check if it's a method on a framework class
                try:
                    method_ref = getattr(obj, method_name, None)
                    if method_ref and self.detector.is_framework_component(method_ref):
                        self.framework_references.add(f"{obj_name}.{method_name}()")
                except (AttributeError, TypeError):
                    pass

    def visit_Attribute(self, node: cst.Attribute) -> None:
        """Detect access to framework object attributes."""
        if isinstance(node.value, cst.Name):
            obj_name = node.value.value
            attr_name = node.attr.value
            obj = self._resolve_name(obj_name)

            if obj and self.detector.is_framework_component(obj):
                self.framework_references.add(f"{obj_name}.{attr_name}")


class FunctionBodyExtractor(cst.CSTVisitor):
    """Visitor that extracts the body of a specific function."""

    def __init__(self, target_function: str):
        super().__init__()
        self.target_function = target_function
        self.function_body = None
        self.found = False

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        if node.name.value == self.target_function:
            self.function_body = node.body
            self.found = True


class FunctionCallExtractor(cst.CSTVisitor):
    """Visitor that extracts all function calls within code."""

    def __init__(
        self, global_namespace: Dict[str, Any], local_namespace: Dict[str, Any] = None
    ):
        super().__init__()
        self.global_namespace = global_namespace
        self.local_namespace = local_namespace or {}
        self.called_functions = set()

    def _resolve_name(self, name: str) -> Optional[Any]:
        """Try to resolve a name to its actual object."""
        if name in self.local_namespace:
            return self.local_namespace.get(name)
        return self.global_namespace.get(name)

    def visit_Call(self, node: cst.Call) -> None:
        """Extract called functions."""
        if isinstance(node.func, cst.Name):
            # Direct function call: some_function()
            func_name = node.func.value
            func_obj = self._resolve_name(func_name)

            if func_obj and callable(func_obj):
                self.called_functions.add(func_obj)

        elif isinstance(node.func, cst.Attribute):
            # Method call: obj.method()
            if isinstance(node.func.value, cst.Name):
                obj_name = node.func.value.value
                method_name = node.func.attr.value
                obj = self._resolve_name(obj_name)

                try:
                    method_obj = getattr(obj, method_name, None)
                    if method_obj and callable(method_obj):
                        self.called_functions.add(method_obj)
                except (AttributeError, TypeError):
                    pass

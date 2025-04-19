from reactive.meta.detector import FrameworkDetector

# ===== Framework A Setup =====
# Create a detector specific to 'Framework A'
detector_a = FrameworkDetector("framework_a")
# Get the decorator and metaclass provided by this detector
framework_a_func = detector_a.get_function_decorator()
FrameworkAClass = detector_a.get_metaclass()

# ===== Framework B Setup =====
# Create a separate detector for 'Framework B'
detector_b = FrameworkDetector("framework_b")
framework_b_func = detector_b.get_function_decorator()
FrameworkBClass = detector_b.get_metaclass()


# ===== Define Framework A Components =====
# Mark this function as belonging to Framework A
@framework_a_func
def component_a_util():
    """Framework A utility function."""
    return "Util A"


# Mark this class as belonging to Framework A using the metaclass
class ComponentA(metaclass=FrameworkAClass):
    """Framework A class."""

    def process(self):
        # This method is automatically marked as a component method by the metaclass
        return "Processed by A"


# ===== Define Framework B Components =====
# Mark this function as belonging to Framework B
@framework_b_func
def component_b_service():
    """Framework B service function."""
    return "Service B"


# Mark this class as belonging to Framework B
class ComponentB(metaclass=FrameworkBClass):
    """Framework B class."""

    def __init__(self):
        # The metaclass will trigger analysis of __init__
        # It calls a component from Framework A.
        self.util_a_result = component_a_util()

    def execute(self):
        # This method is automatically marked by the metaclass
        return f"Executed by B (using {self.util_a_result})"


# ===== Example of Stacked Decorators =====
# This function is marked as a component by both frameworks
@framework_a_func
@framework_b_func
def multi_framework_function():
    """
    A function recognized by both Framework A and Framework B detectors.
    It also uses components from both frameworks internally.
    """
    res_a = component_a_util()  # Usage of Framework A component
    res_b = component_b_service()  # Usage of Framework B component
    return f"Multi: {res_a}, {res_b}"


# ===== User Code Example Using Components =====


# User code that is also marked as a Framework A component.
# This allows detector_a to analyze its dependencies on other Framework A components.
@framework_a_func
def process_data():
    """Example user function using components from both frameworks."""
    # Using Framework A utility
    result_a = component_a_util()

    # Using Framework B service (detector A won't mark this as an 'A' reference)
    result_b = component_b_service()

    # Using Framework A class
    instance_a = ComponentA()
    processed = instance_a.process()  # Call to a Framework A method

    # Using Framework B class
    instance_b = ComponentB()
    executed = instance_b.execute()  # Call to a Framework B method

    return f"Data: {result_a}, {result_b}, {processed}, {executed}"


# ===== Example Execution =====

if __name__ == "__main__":
    print("--- Framework Component Identification ---")

    # Verify that the function with stacked decorators is recognized by both detectors
    is_a_component = detector_a.is_framework_component(multi_framework_function)
    is_b_component = detector_b.is_framework_component(multi_framework_function)
    print(f"Is multi_framework_function a Framework A component? {is_a_component}")
    print(f"Is multi_framework_function a Framework B component? {is_b_component}")

    print("\n--- Framework Reference Detection ---")

    # Analyze the multi-framework function using each detector
    print("\nFramework A references detected in multi_framework_function():")
    # Retrieves references calculated when the decorator was applied
    refs_a_in_multi = detector_a.get_framework_references(multi_framework_function)
    # Detector A should find references to components marked by detector_a
    for ref in sorted(refs_a_in_multi or []):
        print(f"  - {ref}")  # Expected: component_a_util()

    print("\nFramework B references detected in multi_framework_function():")
    refs_b_in_multi = detector_b.get_framework_references(multi_framework_function)
    # Detector B should find references to components marked by detector_b
    for ref in sorted(refs_b_in_multi or []):
        print(f"  - {ref}")  # Expected: component_b_service()

    # Analyze the user function `process_data` using Detector A
    print("\nFramework A references detected in process_data():")
    # This retrieves references found by detector_a when @framework_a_func was applied
    # It includes direct calls to A components and indirect calls discovered via recursion.
    refs_a_in_process = detector_a.get_framework_references(process_data)
    # Expected: component_a_util() (direct), ComponentA() (direct), ComponentA.process() (direct)
    # component_a_util() might appear again if found indirectly via ComponentB analysis,
    # but deduplication should handle it.
    for ref in sorted(refs_a_in_process or []):
        print(f"  - {ref}")

    # Analyze ComponentB's __init__ method using Detector A
    # This shows Detector A can analyze code even if it's not marked as an 'A' component,
    # typically done when tracing calls from an 'A' component.
    print("\nFramework A references detected in ComponentB (via __init__ analysis):")
    refs_a_in_b = detector_a.detect_framework_usage(
        ComponentB
    )  # Analyze the class (__init__)
    # Detector A finds the usage of component_a_util (an 'A' component) inside ComponentB's __init__
    for ref in sorted(refs_a_in_b or []):
        print(f"  - {ref}")  # Expected: component_a_util()

    # Analyze ComponentB's __init__ using Detector B
    print("\nFramework B references detected in ComponentB (via __init__ analysis):")
    # Detector B analyzed ComponentB via its metaclass. Get the stored result.
    refs_b_in_b = detector_b.get_framework_references(ComponentB)
    # Detector B should not find any 'B' references inside __init__
    if not refs_b_in_b:
        print("  - (None)")
    else:
        for ref in sorted(refs_b_in_b):
            print(f"  - {ref}")  # Expected: (None)

    # Analyze ComponentB.execute using Detector B
    print("\nFramework B references detected in ComponentB.execute():")
    # Detector B analyzed ComponentB.execute via its metaclass. Get the stored result.
    refs_b_in_b_exec = detector_b.get_framework_references(ComponentB.execute)
    # Detector B should not find any 'B' references inside execute
    if not refs_b_in_b_exec:
        print("  - (None)")
    else:
        for ref in sorted(refs_b_in_b_exec):
            print(f"  - {ref}")  # Expected: (None)

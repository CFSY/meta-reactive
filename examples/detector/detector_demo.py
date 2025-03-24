from reactive.meta.detector import FrameworkDetector

# Create a framework detector
detector = FrameworkDetector("myframework")

# Get the function decorator and metaclass
framework_function = detector.get_function_decorator()
FrameworkClass = detector.get_metaclass()


# Define framework components
@framework_function
def utility_func():
    """Example framework utility function."""
    return "utility result"


# function that calls a framework function
def indirect_utility_func():
    utility_func()


class ExampleFrameworkClass(metaclass=FrameworkClass):
    """Example framework class."""

    def __init__(self):
        self.name = "example framework class"

    def process(self):
        return f"{self.name} processed"

    def another_method(self):
        # This uses another framework component
        return f"{self.name} {utility_func()}"


# Create an instance for testing
fw_class_instance = ExampleFrameworkClass()


# Define user code that uses framework components
@framework_function
def process_data():
    """Example function that uses framework components."""
    # Using framework utility function
    result = utility_func()

    # Using framework class method
    fw_class_instance.process()

    # Using framework class attribute
    name = fw_class_instance.name

    regular_var = "non_framework_var"
    return result


class ExampleFrameworkClass2(metaclass=FrameworkClass):
    """Example class that uses framework components."""

    def __init__(self):
        self.name = "example framework class 2"
        self.utility = utility_func()
        self.regular_var = "non_framework_var"

        # Use another framework component
        instance = ExampleFrameworkClass()
        instance.process()

    def do_something(self):
        # This method also uses framework components
        return f"{self.name} {indirect_utility_func()}"


# ===== Example Execution =====

if __name__ == "__main__":
    # Testing framework detection in functions
    print("\nFramework references detected in process_data():")
    refs = detector.get_framework_references(process_data)
    for ref in sorted(refs):
        print(f"  - {ref}")

    # Testing framework detection in classes
    print("\nFramework references detected in ExampleFrameworkClass2:")
    refs = detector.get_framework_references(ExampleFrameworkClass2)
    for ref in sorted(refs):
        print(f"  - {ref}")

    # Testing framework detection in class methods
    print("\nFramework references detected in ExampleFrameworkClass2.do_something():")
    method = ExampleFrameworkClass2.do_something
    refs = detector.get_framework_references(method)
    for ref in sorted(refs):
        print(f"  - {ref}")

    # Using direct API for detection
    refs = detector.detect_framework_usage(process_data)
    print("\nDirect API detection results for process_data():")
    for ref in sorted(refs):
        print(f"  - {ref}")

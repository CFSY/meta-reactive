from src.reactive.meta.detector import FrameworkDetector

# Create a framework detector for the meta API
detector = FrameworkDetector("reactive_meta")
framework_function = detector.get_function_decorator()
FrameworkClass = detector.get_metaclass()

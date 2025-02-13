from setuptools import setup, find_packages

setup(
    name="reactive_framework",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)

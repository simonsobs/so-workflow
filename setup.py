from setuptools import find_packages, setup

setup(
    name="so-workflow",
    version="0.1.0",
    packages=find_packages(where=".", exclude="tests"),
)

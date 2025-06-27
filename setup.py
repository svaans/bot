from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "fast_indicators.fast_indicators_ext",
        ["fast_indicators/fast_indicators.cpp"],
    )
]

setup(
    name="fast_indicators",
    version="0.1.0",
    packages=["fast_indicators"],
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)
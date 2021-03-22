from Cython.Build import cythonize
from setuptools import Extension

extensions = [
    Extension(
        "diamond_miner.utilities.format",
        ["diamond_miner/utilities/format.pyx"],
    ),
    Extension(
        "diamond_miner.utilities.parameter_grid",
        ["diamond_miner/utilities/parameter_grid.pyx"],
    ),
]


def build(setup_kwargs):
    setup_kwargs.update({"ext_modules": cythonize(extensions)})

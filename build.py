from distutils.command.build_ext import build_ext

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
    setup_kwargs.update(
        {
            "cmdclass": {"build_ext": build_ext},
            "ext_modules": cythonize(extensions, language_level=3),
        }
    )

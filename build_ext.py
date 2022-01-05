# For some reasons `from setuptools.command.build_ext import build_ext`
# causes errors in the wheels built with cibuildhweel.
# ModuleNotFoundError: No module named 'diamond_miner.mappers'
from distutils.command.build_ext import build_ext

from Cython.Build import cythonize


def build(setup_kwargs):
    setup_kwargs.update(
        {
            "cmdclass": {"build_ext": build_ext},
            "ext_modules": cythonize(
                "diamond_miner/**/*.pyx",
                compiler_directives={"binding": True, "embedsignature": True},
                language_level=3,
            ),
        }
    )

from Cython.Build import cythonize
from setuptools.command.build_ext import build_ext


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

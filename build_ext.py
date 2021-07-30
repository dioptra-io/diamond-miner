from distutils.command.build_ext import build_ext  # type: ignore

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

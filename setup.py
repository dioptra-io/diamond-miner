from Cython.Build import cythonize
from setuptools import find_packages, setup

setup(
    name="diamond-miner",
    version="1.0.5",
    license="MIT",
    description="High-speed, Internet-scale, load-balanced paths discovery.",
    author="Kevin Vermeulen, Matthieu Gouel, Maxime Mouchet",
    url="https://github.com/dioptra-io/diamond-miner",
    packages=find_packages(),
    ext_modules=cythonize(
        "diamond_miner/**/*.pyx",
        compiler_directives={"binding": True, "embedsignature": True},
        language_level=3,
    ),
    python_requires=">=3.10",
    install_requires=[
        "pych-client~=0.4.0",
        "pygfc~=1.0.5",
        "zstandard>=0.15.2,<0.19.0",
    ],
    extras_require={
        "dev": [
            "bumpversion~=0.6.0",
            "coverage[toml]~=7.1.0",
            "hypothesis~=6.68.2",
            "mkdocs-bibtex~=2.8.13",
            "mkdocs-material~=9.0.13",
            "mkdocstrings[python]~=0.20.0",
            "mypy~=1.0.1",
            "pytest~=7.2.1",
            "pytest-cov~=4.0.0",
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Topic :: Internet",
        "Typing :: Typed",
    ],
)

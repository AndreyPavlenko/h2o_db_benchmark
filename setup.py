import runpy
from pathlib import Path
from setuptools import setup, find_namespace_packages

root = Path(__file__).resolve().parent

with open(root / "README.md", encoding="utf-8") as f:
    long_description = f.read()


def parse_reqs(name):
    with open(root / "requirements" / name, "r") as f:
        return f.readlines()


name = "omniscripts_benchmarks.h2o"
version = runpy.run_path(root / "omniscripts_benchmarks" / "h2o" / "__version__.py")["__version__"]

setup(
    name=name,
    version=version,
    description="Implementation of H2O benchmark for key backends",
    long_description=long_description,
    url="https://github.com/intel-ai/h2o_db_benchmark",
    packages=[
        *find_namespace_packages(include=["omniscripts_benchmarks.*"]),
    ],
    install_requires=parse_reqs("base.txt"),
    python_requires=">=3.8",
)

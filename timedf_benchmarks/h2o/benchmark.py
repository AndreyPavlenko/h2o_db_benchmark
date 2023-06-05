import gc
import argparse
import importlib

from timedf import BaseBenchmark, BenchmarkResults, tm
from timedf.pandas_backend import Backend

from .h2o_utils import get_load_info, H2OBackend


def main_groupby(paths, backend):
    with tm.timeit("groupby"):
        with tm.timeit("load_data"):
            df = backend.load_groupby_data(paths)
            df = Backend.trigger_loading(df)

        with tm.timeit("ops"):
            for name, q in backend.name2groupby_query.items():
                gc.collect()

                with tm.timeit(name):
                    # Force action
                    Backend.trigger_execution(q(df))


def main_join(paths, backend):
    with tm.timeit("join"):
        with tm.timeit("load_data"):
            data = backend.load_join_data(paths)
            data = {name: Backend.trigger_loading(df) for name, df in data.items()}

        with tm.timeit("ops"):
            for name, q in backend.name2join_query.items():
                gc.collect()
                with tm.timeit(name):
                    # Force action
                    Backend.trigger_execution(q(data))


def main(data_path, backend, size, task):
    paths = get_load_info(data_path, size=size)
    if task == "all" or task == "groupby":
        main_groupby(paths, backend=backend)
    if task == "all" or task == "join":
        main_join(paths, backend=backend)


# Stores non-pandas implemenations
backend2impl = {"polars": "h2o_polars"}


def get_impl_module(backend):
    # replacement example: omniscript.benchmarks.(xxx -> h2o_pandas)
    name_tokens = __name__.split(".")
    name_tokens[-1] = backend2impl.get(backend, "h2o_pandas")
    impl_module_path = ".".join(name_tokens)
    return importlib.import_module(impl_module_path)


class Benchmark(BaseBenchmark):
    __params__ = ("size", "task", "modin_exp_gb")

    def add_benchmark_args(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "-size",
            choices=["small", "medium", "large"],
            default="small",
            help="Dataset size from 1e7 to 1e9.",
        )

        parser.add_argument(
            "-task",
            choices=["groupby", "join", "all"],
            default="all",
            help="Task to run",
        )

        parser.add_argument("-modin_exp_gb", default=False, action="store_true")

    def run_benchmark(self, params) -> BenchmarkResults:
        backend: H2OBackend = get_impl_module(params["backend"]).H2OBackendImpl(
            modin_exp_gb=params["modin_exp_gb"]
        )

        main(
            data_path=params["data_file"],
            backend=backend,
            size=params["size"],
            task=params["task"],
        )
        super().run_benchmark(params)
        measurement2time = tm.get_results()
        print(measurement2time)
        return BenchmarkResults(measurement2time)

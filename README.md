# h2o_db_benchmark
Repository with our reproduction of H2O db benchmark for key backends, including modin

### How to run

1. Install timedf from https://github.com/intel-ai/timedf
2. Install this library with `pip install .`
3. Run benchmark like any other `timedf` benchmark with `benchmark-run h2o -data_file PATH`

### H2O benchmark

This benchmark is based on H2O benchmarks, that were perfomed for key backends.
The benchmarks cover basic groupby and join operatoin.
There are 10 groupby queries and 5 join queries.

### Files
1. `benchmark.py` - main benchmark.
2. `h2o_pandas.py` - pandas backend implementation.
3. `h2o_polars.py` - polars backend implementation.
4. `h2o_utils.py` - utils for benchmark.

### Difference from original H2O
1. Instead of performing operation twice we support multiple iterations for the whole benchmark. Reasoning: it is unclear why would we run the same operation twice during data processing on exactly the same data, so there are no real use-case.

### Known problems
We don't support `benchmark-load` yet, mainly because data generation is written in R

### Unavailable operations
1. Currently HDK doesn't support groupby `q10`.

### Supported backends:
- Pandas
- Modin
- Polars

### Sources
- https://h2oai.github.io/db-benchmark/
- https://github.com/h2oai/db-benchmark

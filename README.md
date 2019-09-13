# clickhouse_benchmark_tools

* `explain.py` - run postgres queries on `clickhouse_fdw` and extract 'Remove SQL' statements
* `make_working.py` - create a list of queries that return something from full list of queries, creates `ch_working.out` file
* `benchmark.py` - takes list of queries and benchmarking them on some table.

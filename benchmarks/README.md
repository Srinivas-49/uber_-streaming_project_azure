# Spark/Databricks performance study

`databricks_50m.py` is an intentionally **unexecuted** cloud benchmarking harness for 1–100 million generated synthetic ride records. It is not a result. The default **50M rows could incur substantial cloud compute and storage costs**.

Set `BENCHMARK_SCRATCH_SCHEMA` and optionally `BENCHMARK_ROWS`. Review costs, data retention and timeouts before running the notebook in an isolated, approved Databricks workspace.

The script measures 5 trials of unpartitioned and monthly partitioned queries, optional Z-ORDER, and joins with broadcast disabled/enabled. Record warm/cold-cache conditions, runtime, DBR, cluster SKU, worker count, DBUs, volumes, file counts and median/p95. No timing/percentage should appear on a CV until reproducibly collected. Remove scratch tables afterward.

# Databricks Spark/Delta performance benchmark — execution runbook

The source notebook is [spark_delta_benchmark_notebook.py](spark_delta_benchmark_notebook.py). It benchmarks **real PySpark/Delta operations inside an Azure Databricks workspace**; it is **not** the local Python/SQLite benchmark and has not yet been executed.

## Before running
1. Confirm Databricks workspace access, catalog/schema CREATE TABLE and SELECT permissions, and cost approval.
2. Import the Python file as a notebook or add it to a Databricks workspace notebook path. Use a permitted cluster or serverless notebook environment supporting Delta OPTIMIZE ZORDER.
3. Set notebook widgets:
   - `scratch_schema`: an isolated scratch Unity Catalog schema, such as `portfolio.benchmark`
   - `rows`: start at `1000000`; increase to `50000000` only after estimating DBUs, storage and run time
   - `trials`: `5`
   - `cleanup`: `yes` (drops generated scratch tables after completion)
4. Review data retention, cluster auto-termination, budget alerts, all access grants and the generated tables before approving any paid run.

## Runs and metrics
- Unpartitioned baseline vs month-partitioned Delta; same filter predicate and output.
- Partitioned Delta before and after OPTIMIZE ZORDER by city ID.
- Join with Spark automatic broadcast disabled vs enabled.
- Captures five elapsed-time samples, median and approximate p95; it prints `BENCHMARK_FINAL_JSON`.
- Capture cluster type, Spark/Databricks Runtime, worker count, DBUs, cache settings, Spark UI query plans, partition/file sizes and estimated cost separately.

## Verification and resume policy
Only report a run once a workspace Job/Notebook URL, git SHA, output, schema/row counts and billing/cluster metadata exist. The 50M-row number is a **configurable test size**, not a completed run. Do not claim speedup or savings from these files alone.

## Deployment prerequisites
The Databricks bundle is defined in [../databricks.yml](../databricks.yml), and the separate manual GitHub Actions workflow can validate/deploy it using GitHub OIDC. It cannot run without a configured workspace host, Databricks service principal, Unity Catalog catalog and alert address. The scheduled job is PAUSED by default. Repository integration blockers are documented in [../docs/BUNDLE_DEPLOYMENT_GUIDE.md](../docs/BUNDLE_DEPLOYMENT_GUIDE.md).

# Databricks notebook source
"""Controlled Delta optimization benchmark. Execute in Azure Databricks.
The defaults are low-cost preflight settings; scale only after approval.
A local Python/SQLite result must never be passed off as this experiment.
"""
import json
import statistics
import time
import uuid
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

dbutils.widgets.text("scratch_schema", "")
dbutils.widgets.text("rows", "1000000")
dbutils.widgets.text("trials", "5")
dbutils.widgets.dropdown("cleanup", "yes", ["yes", "no"])
database = dbutils.widgets.get("scratch_schema").strip()
rows = int(dbutils.widgets.get("rows"))
trials = int(dbutils.widgets.get("trials"))
cleanup = dbutils.widgets.get("cleanup") == "yes"

if not database or len(database.split(".")) != 2:
    raise ValueError("Set scratch_schema to existing catalog.schema")
if not all(piece.replace("_", "").isalnum() for piece in database.split(".")):
    raise ValueError("Invalid catalog/schema; only letters, numbers and underscores")
if not (1000 <= rows <= 100000000) or not (3 <= trials <= 20):
    raise ValueError("rows must be 1k-100M and trials 3-20")
catalog, schema = database.split(".")
quoted_db = ".".join("`" + part + "`" for part in (catalog, schema))
run_id = "cvbench_" + uuid.uuid4().hex[:10]
tables = {name: f"{quoted_db}.`{run_id}_{name}`"
          for name in ("baseline", "partitioned", "dimension")}
results = {"run_id": run_id, "rows": rows, "trials": trials,
           "scratch_schema": database, "measurements": []}
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_db}")

def trial(label, query):
    measured = []
    for index in range(trials):
        start = time.perf_counter()
        row = spark.sql(query).collect()[0]
        measured.append(round(time.perf_counter() - start, 4))
    sorted_times = sorted(measured)
    result = {"scenario": label, "seconds": measured,
              "median_seconds": round(statistics.median(measured), 4),
              "p95_seconds_nearest_rank": sorted_times[min(trials - 1, int(0.95 * trials + 0.9999) - 1)],
              "result": str(row)}
    results["measurements"].append(result)
    print("BENCHMARK_RESULT", json.dumps(result, sort_keys=True))

started = time.perf_counter()
saved_broadcast = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
try:
    data = (spark.range(rows).withColumnRenamed("id", "ride_id")
            .withColumn("city_id", (F.col("ride_id") % 4096).cast("int"))
            .withColumn("ride_date", F.date_add(
                F.lit("2025-01-01").cast("date"), (F.col("ride_id") % 365).cast("int")))
            .withColumn("ride_month", F.date_format(F.col("ride_date"), "yyyy-MM"))
            .withColumn("fare", ((F.col("ride_id") % 99) + 1).cast("double")))
    data.write.format("delta").mode("overwrite").saveAsTable(tables["baseline"])
    data.write.format("delta").mode("overwrite").partitionBy("ride_month").saveAsTable(tables["partitioned"])
    dims = (spark.range(4096).withColumnRenamed("id", "city_id")
            .withColumn("segment", (F.col("city_id") % 16).cast("int")))
    dims.write.format("delta").mode("overwrite").saveAsTable(tables["dimension"])
    # Same predicate for fair before-and-after layout comparison.
    template = ("SELECT COUNT(*) AS n, SUM(fare) AS revenue FROM {table} "
                "WHERE ride_month='2025-02' AND city_id=42")
    trial("unpartitioned", template.format(table=tables["baseline"]))
    trial("month_partitioned", template.format(table=tables["partitioned"]))
    spark.sql(f"OPTIMIZE {tables['partitioned']} ZORDER BY (city_id)")
    trial("partitioned_plus_zorder", template.format(table=tables["partitioned"]))
    join_template = ("SELECT COUNT(*), SUM(f.fare) FROM {fact} f "
                     "JOIN {dim} d ON f.city_id=d.city_id "
                     "WHERE f.ride_month='2025-02' AND d.segment=10")
    # These runs demonstrate join-threshold effects, not guaranteed speedups.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    trial("join_auto_broadcast_disabled", join_template.format(
        fact=tables["baseline"], dim=tables["dimension"]))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(32 * 1024 * 1024))
    trial("join_auto_broadcast_enabled", join_template.format(
        fact=tables["baseline"], dim=tables["dimension"]))
    results["total_seconds"] = round(time.perf_counter() - started, 3)
    print("BENCHMARK_FINAL_JSON", json.dumps(results, sort_keys=True))
finally:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", saved_broadcast)
    if cleanup:
        for name, table in tables.items():
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception as error:
                print("CLEANUP_WARNING", name, repr(error))

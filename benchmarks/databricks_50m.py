"""Databricks-only reproducible performance experiment. Not run in production.
Run in a secured Databricks notebook attached to a permitted Spark cluster.
Requires explicit benchmark row count and a writable scratch schema; will create tables.
"""
import os
import time
from pyspark.sql import functions as F

rows = int(os.environ.get("BENCHMARK_ROWS", "50000000"))
if not 1 <= rows <= 100000000:
    raise ValueError("BENCHMARK_ROWS must be between 1 and 100 million")
database = os.environ["BENCHMARK_SCRATCH_SCHEMA"]  # e.g. mycatalog.benchmark_schema
if not all(part.replace("_", "").isalnum() for part in database.split(".")):
    raise ValueError("Use only safe Unity Catalog identifier components")
base = f"{database}.ride_benchmark"
baseline = f"{database}.ride_baseline"
partitioned = f"{database}.ride_partitioned"
dimension = f"{database}.ride_dimension"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")

print("Experiment start", {"rows": rows, "schema": database, "baseline": baseline})
events = (spark.range(rows).withColumnRenamed("id", "ride_id")
 .withColumn("city_id", (F.col("ride_id") % 256).cast("int"))
 .withColumn("ride_month", F.date_format(
    F.date_add(F.lit("2025-01-01").cast("date"), (F.col("ride_id") % 365).cast("int")), "yyyy-MM"))
 .withColumn("fare", (F.col("ride_id") % 100).cast("double")))
events.write.format("delta").mode("overwrite").saveAsTable(baseline)
events.repartition("ride_month").write.format("delta").mode("overwrite").partitionBy("ride_month").saveAsTable(partitioned)
dims = spark.range(256).withColumnRenamed("id", "city_id").withColumn("segment", (F.col("city_id") % 8).cast("int"))
dims.write.mode("overwrite").format("delta").saveAsTable(dimension)

def measure(name, sql, repeat=5):
    values = []
    for index in range(repeat):
        start = time.perf_counter()
        result = spark.sql(sql).collect()  # force execution, not only lazy plan
        seconds = time.perf_counter() - start
        values.append(round(seconds, 4))
    sorted_values = sorted(values)
    print({"test": name, "rows": rows, "runs_sec": values,
           "median_sec": sorted_values[len(values)//2],
           "p95_sec_approx": sorted_values[-1]})

query = "SELECT COUNT(*), SUM(fare) FROM {table} WHERE ride_month='2025-02'"
measure("baseline_unpartitioned", query.format(table=baseline))
measure("month_partitioned", query.format(table=partitioned))
spark.sql(f"OPTIMIZE {partitioned} ZORDER BY (city_id)")
measure("partitioned_and_zordered", query.format(table=partitioned))
join = ("SELECT SUM(e.fare) FROM {table} e JOIN {dimension} d "
        "ON e.city_id = d.city_id WHERE d.segment=4")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
measure("join_broadcast_disabled", join.format(table=baseline, dimension=dimension))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(32 * 1024 * 1024))
measure("join_broadcast_enabled", join.format(table=baseline, dimension=dimension))
print("Report DBR version, cluster node SKU, workers, DBUs, region, costs and retry behavior separately.")

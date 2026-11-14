# Databricks notebook source
# MAGIC %md
# MAGIC # Uber final gate: Bronze → Silver → Gold reconciliation
# MAGIC Fails visibly if the expected synthetic run is corrupt or incomplete.

# COMMAND ----------
from pyspark.sql import functions as F
expected = {
    "bronze_ride_events": 264,
    "silver_ride_quarantine": 1,
    "silver_ride_history": 261,
    "silver_current_rides": 200,
    "gold_ride_status": 3,
    "gold_zone_kpis": 2,
}
spark.sql("USE SCHEMA uber_portfolio")
for table, count in expected.items():
    observed = spark.table(table).count()
    assert observed == count, f"{table}: expected {count}, got {observed}"

current = spark.table("silver_current_rides")
assert current.groupBy("ride_id").count().filter("count > 1").count() == 0
assert current.filter("ride_id = 'ride-00000' AND status = 'completed'").count() == 1
source_revenue = current.agg(F.sum(F.when(F.col("status") == "completed", "fare_pennies").otherwise(0))).first()[0]
status_revenue = spark.table("gold_ride_status").agg(F.sum("revenue_pennies")).first()[0]
zone_revenue = spark.table("gold_zone_kpis").agg(F.sum("revenue_pennies")).first()[0]
assert source_revenue == status_revenue == zone_revenue, "Gold ride revenue mismatch"

audit = (spark.createDataFrame(list(expected.items()), "test_name string, measured long")
         .withColumn("checked_at", F.current_timestamp())
         .withColumn("result", F.lit("passed"))
         .select("checked_at", "test_name", "measured", "result"))
audit.write.format("delta").mode("append").saveAsTable("ops_quality_audit")
print("PASS: ride quality, CDC ordering, 6 data contracts and financial reconciliation")
display(audit)

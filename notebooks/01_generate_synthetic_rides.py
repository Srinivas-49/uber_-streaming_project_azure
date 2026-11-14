# Databricks notebook source
# MAGIC %md
# MAGIC # Uber: Spark fundamentals and synthetic Bronze ingestion
# MAGIC Create source data, transformations and managed Delta tables on serverless.
# MAGIC Uses no API keys or real passenger / driver information.

# COMMAND ----------
from pyspark.sql import functions as F

SCHEMA_NAME = "uber_portfolio"
RIDE_COUNT = 200
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------
source = spark.range(RIDE_COUNT).select(
    F.format_string("ride-%05d", "id").alias("ride_id"),
    F.format_string("passenger-%03d", F.col("id") % 40).alias("passenger_id"),
    F.format_string("driver-%03d", F.col("id") % 25).alias("driver_id"),
    F.format_string("request-%05d", "id").alias("event_id"),
    F.lit("requested").alias("status"),
    F.lit("2026-11-14T09:00:00Z").alias("event_time"),
    F.lit(0).cast("long").alias("fare_pennies"),
    F.when(F.col("id") % 2 == 0, "north").otherwise("south").alias("zone")
)
ride_number = F.col("ride_id").substr(6, 5).cast("int")
complete = (source.filter(ride_number % 5 == 0)
    .withColumn("event_id", F.concat(F.lit("complete-"), "ride_id"))
    .withColumn("status", F.lit("completed"))
    .withColumn("event_time", F.lit("2026-11-14T10:00:00Z"))
    .withColumn("fare_pennies", F.lit(1200) + ride_number))
cancel = (source.filter(ride_number % 10 == 1)
    .withColumn("event_id", F.concat(F.lit("cancel-"), "ride_id"))
    .withColumn("status", F.lit("cancelled"))
    .withColumn("event_time", F.lit("2026-11-14T09:45:00Z")))
late = (source.filter(F.col("ride_id") == "ride-00000")
    .withColumn("event_id", F.lit("late-ride-00000"))
    .withColumn("status", F.lit("cancelled"))
    .withColumn("event_time", F.lit("2026-11-14T08:30:00Z")))
invalid = (source.filter(F.col("ride_id") == "ride-00002")
    .withColumn("event_id", F.lit("invalid-ride-00002"))
    .withColumn("fare_pennies", F.lit(-20)))
duplicates = source.filter(F.col("ride_id").isin("ride-00000", "ride-00001"))
events = (source.unionByName(complete).unionByName(cancel).unionByName(late)
          .unionByName(invalid).unionByName(duplicates))

# COMMAND ----------
events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_ride_events")
assert spark.table("bronze_ride_events").count() == 264
print("PASS: 264 synthetic events, including duplicates, late update and invalid fare")
display(spark.table("bronze_ride_events").orderBy("ride_id", "event_time").limit(25))

# COMMAND ----------
# MAGIC %md
# MAGIC Explore counts by status and DESCRIBE HISTORY bronze_ride_events.
# MAGIC Then run Notebook 02 for validation, time-aware state and analytics.

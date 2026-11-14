# Databricks notebook source
# MAGIC %md
# MAGIC # Uber: Spark validation, SCD2 history and analytical Gold tables
# MAGIC Run notebook 01 first. Bounded synthetic Delta batch, not live Event Hubs.

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DecimalType

spark.sql("USE SCHEMA uber_portfolio")
raw = spark.table("bronze_ride_events")
deduplicated = raw.dropDuplicates(["event_id"])
reason = F.concat_ws("; ",
    F.when(F.col("ride_id").isNull() | (F.length(F.trim("ride_id")) == 0), "missing ride_id"),
    F.when(F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "missing event_id"),
    F.when(~F.col("status").isin("requested", "completed", "cancelled"), "invalid status"),
    F.when(F.col("fare_pennies").isNull() | (F.col("fare_pennies") < 0), "invalid fare"),
    F.when(F.to_timestamp("event_time").isNull(), "invalid event_time"))
tagged = deduplicated.withColumn("quality_issue", reason)
quarantine = tagged.filter(F.length("quality_issue") > 0)
valid = (tagged.filter(F.length("quality_issue") == 0)
         .withColumn("event_timestamp", F.to_timestamp("event_time"))
         .drop("quality_issue"))
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_ride_quarantine")
valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_ride_history")

# COMMAND ----------
# This reconstructs ordered event history; a production CDC flow needs persistent
# state, source change semantics, retention policy and collision handling.
ordered = Window.partitionBy("ride_id").orderBy("event_timestamp", "event_id")
scd = (valid.withColumn("valid_from", F.col("event_timestamp"))
       .withColumn("valid_to", F.lead("event_timestamp").over(ordered))
       .withColumn("is_current", F.col("valid_to").isNull()))
scd.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_ride_scd2")
current_window = Window.partitionBy("ride_id").orderBy(
    F.col("event_timestamp").desc(), F.col("event_id").desc())
current = (valid.withColumn("rank", F.row_number().over(current_window))
           .filter(F.col("rank") == 1).drop("rank"))
current.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_current_rides")

# COMMAND ----------
completed_pennies = F.when(F.col("status") == "completed", F.col("fare_pennies")).otherwise(F.lit(0))
by_status = (current.groupBy("status")
    .agg(F.count("*").alias("rides"), F.sum(completed_pennies).alias("revenue_pennies"))
    .withColumn("revenue_gbp", (F.col("revenue_pennies") / 100).cast(DecimalType(18,2))))
by_zone = (current.groupBy("zone")
    .agg(F.count("*").alias("rides"),
         F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
         F.sum(completed_pennies).alias("revenue_pennies"))
    .withColumn("revenue_gbp", (F.col("revenue_pennies") / 100).cast(DecimalType(18,2))))
by_status.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_ride_status")
by_zone.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_zone_kpis")

# COMMAND ----------
assert raw.count() == 264
assert quarantine.count() == 1
assert valid.count() == 261
assert current.count() == 200
assert current.filter("status = 'completed'").count() == 40
assert current.filter("status = 'cancelled'").count() == 20
assert current.filter("ride_id = 'ride-00000' AND status = 'completed'").count() == 1
assert scd.filter("is_current").count() == 200
a = current.agg(F.sum(completed_pennies).alias("total")).first()["total"]
b = by_status.agg(F.sum("revenue_pennies").alias("total")).first()["total"]
c = by_zone.agg(F.sum("revenue_pennies").alias("total")).first()["total"]
assert a == b == c, "Gold revenue mismatch"
print("PASS: 264 Bronze, 261 accepted, 1 quarantined, 200 current ride records")
display(spark.table("gold_ride_status"))
display(spark.table("gold_zone_kpis"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reliability limits
# MAGIC The notebook uses repeatable full recomputation over synthetic data.
# MAGIC It does not guarantee exactly-once streaming, real-time alerts or access controls.

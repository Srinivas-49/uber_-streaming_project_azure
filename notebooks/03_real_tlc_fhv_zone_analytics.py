# Databricks notebook source
# MAGIC %md
# MAGIC # Real-world rides: NYC TLC high-volume FHV + taxi zones
# MAGIC Public TLC For-Hire Vehicle monthly Parquet, not private Uber data.
# MAGIC Manual import to a UC Volume is required. No huge downloads on notebook run.
# MAGIC Execute a bounded subset on Free Edition, then scale by month only in
# MAGIC an environment with approved compute budget.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

dbutils.widgets.text("landing_root", "")
dbutils.widgets.text("sample_limit", "50000")
root = dbutils.widgets.get("landing_root").rstrip("/")
sample_limit = int(dbutils.widgets.get("sample_limit"))
if not root.startswith("/Volumes/"):
    raise ValueError("Set landing_root to an existing UC Volume directory containing trips/ and reference/")
if not 1 <= sample_limit <= 200_000:
    raise ValueError("For Free Edition sample_limit must be 1..200000; scaling needs a separate approved workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS uber_tlc_public")
spark.sql("USE SCHEMA uber_tlc_public")

# COMMAND ----------
# Read a published monthly high-volume for-hire Parquet and zone lookup CSV.
# Avoid collecting a huge dataset to the driver. The LIMIT is for an illustrative
# subset; sample values are not an unbiased statistical estimate of the whole month.
trip_files = f"{root}/trips/fhvhv_tripdata_*.parquet"
raw = spark.read.parquet(trip_files).limit(sample_limit)
assert raw.limit(1).count() == 1, "No TLC high-volume FHV records found"
required = {"pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"}
missing = required - set(raw.columns)
if missing:
    raise ValueError(f"TLC parquet schema mismatch; missing fields: {sorted(missing)}")
raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_fhv_trips")

zone = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{root}/reference/taxi_zone_lookup.csv")
if not {"LocationID", "Borough", "Zone"}.issubset(set(zone.columns)):
    raise ValueError("The official NYC taxi zone lookup requires LocationID, Borough, Zone")
zone = zone.select(F.col("LocationID").cast("int").alias("zone_id"),
                   F.col("Borough").alias("borough"), F.col("Zone").alias("zone"))
zone.dropDuplicates(["zone_id"]).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable("dim_taxi_zone")

# COMMAND ----------
def optional_float(name):
    return F.col(name).cast("double") if name in raw.columns else F.lit(None).cast("double")

typed = raw.select(
    F.to_timestamp("pickup_datetime").alias("pickup_ts"),
    F.to_timestamp("dropoff_datetime").alias("dropoff_ts"),
    F.col("PULocationID").cast("int").alias("pickup_zone_id"),
    F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
    optional_float("trip_miles").alias("trip_miles"),
    optional_float("base_passenger_fare").alias("base_passenger_fare"),
    optional_float("tips").alias("tips"),
    F.lit("NYC TLC HVFHV").alias("source_dataset"),
)
reason = F.concat_ws("; ",
    F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "invalid timestamp"),
    F.when(F.col("dropoff_ts") < F.col("pickup_ts"), "negative duration"),
    F.when(F.col("pickup_zone_id").isNull(), "unknown pickup zone"),
    F.when(F.col("trip_miles") < 0, "negative distance"),
    F.when(F.col("base_passenger_fare") < 0, "negative base fare"))
tagged = typed.withColumn("rejection_reason", reason)
bad = tagged.filter(F.length("rejection_reason") > 0)
good = (tagged.filter(F.length("rejection_reason") == 0)
        .withColumn("service_date", F.to_date("pickup_ts"))
        .withColumn("duration_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60)
        .drop("rejection_reason"))
bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_quarantine")
good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_fhv_trips")

# COMMAND ----------
lookup = spark.table("dim_taxi_zone")
enriched = good.join(F.broadcast(lookup), good.pickup_zone_id == lookup.zone_id, "left").drop("zone_id")
gold = (enriched.groupBy("service_date", "borough", "zone")
    .agg(F.count("*").alias("trips"),
         F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
         F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
         F.round(F.expr("percentile_approx(duration_minutes, 0.95)"), 2).alias("p95_duration_minutes"),
         F.round(F.sum("base_passenger_fare"), 2).alias("base_fare_total"),
         F.round(F.sum("tips"), 2).alias("tips_total")))
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_zone_daily_kpis")

# COMMAND ----------
bronze_rows = spark.table("bronze_fhv_trips").count()
silver_rows = spark.table("silver_fhv_trips").count()
bad_rows = spark.table("silver_quarantine").count()
assert bronze_rows == silver_rows + bad_rows
assert gold.agg(F.sum("trips")).first()[0] == silver_rows
print("PASS: public FHV data, data-quality separation, zone enrichment, Gold reconciliation",
      {"bronze": bronze_rows, "silver": silver_rows, "quarantine": bad_rows})
display(gold.orderBy(F.col("trips").desc()).limit(25))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Scale and correctness
# MAGIC TLC trip records have no unique event-ID/change log. Do NOT claim this
# MAGIC is an exactly-once streaming CDC pipeline or deduplicate valid identical
# MAGIC trips by a hash. Data is published monthly, not live. This is a sample
# MAGIC subset; raw monthly totals must be validated on a full paid Spark run.

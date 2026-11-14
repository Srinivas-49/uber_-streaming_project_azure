# Databricks notebook source
# MAGIC %md
# MAGIC # Real NYC High-Volume FHV Trips + Taxi Zones + Observed Weather
# MAGIC Official NYC TLC monthly Parquet; Open-Meteo reanalysis weather.
# MAGIC NOT proprietary Uber data and NOT simulated streaming.
# MAGIC Download externally with real_data/tlc_sources.py and upload the three
# MAGIC files to a Unity Catalog Volume. Start with ONE month, not the 244M-row collection.
# MAGIC This reads the whole uploaded Parquet in Spark without collecting to the driver.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

dbutils.widgets.text("landing_root", "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/tlc")
dbutils.widgets.text("month", "2025-01")
root = dbutils.widgets.get("landing_root").rstrip("/")
month = dbutils.widgets.get("month")
import re
if not root.startswith("/Volumes/") or "YOUR_" in root:
    raise ValueError("Set landing_root to a real Unity Catalog Volume upload")
if not re.fullmatch(r"20[12][0-9]-(0[1-9]|1[0-2])", month):
    raise ValueError("Invalid YYYY-MM")
spark.sql("CREATE SCHEMA IF NOT EXISTS uber_real_hvfhv")
spark.sql("USE SCHEMA uber_real_hvfhv")

# COMMAND ----------
raw = spark.read.parquet(f"{root}/fhvhv_tripdata_{month}.parquet")
zone_csv = (spark.read.option("header", "true").option("inferSchema", "false")
            .csv(f"{root}/taxi_zone_lookup.csv"))
weather_json = spark.read.option("multiLine", "true").json(f"{root}/nyc_weather_{month}.json")
if raw.isEmpty() or zone_csv.isEmpty() or weather_json.isEmpty():
    raise ValueError("At least one source is empty")

raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_fhvhv_trips")
zone_csv.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_taxi_zones")
weather_json.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_nyc_weather")

# COMMAND ----------
# TLC parquet schemas may evolve, so confirm required fields by name before
# processing. Preserve all raw fields in Bronze for schema/contract investigations.
names = {c.lower(): c for c in raw.columns}
required = ["pickup_datetime", "dropoff_datetime", "pulocationid", "dolocationid",
            "trip_miles", "base_passenger_fare", "hvfhs_license_num"]
missing = [c for c in required if c not in names]
if missing:
    raise ValueError(f"TLC source schema changed, missing fields: {missing}")

def field(name):
    return F.col(names[name.lower()])

rides = (raw.select(
    field("pickup_datetime").cast("timestamp").alias("pickup_ts"),
    field("dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
    field("PULocationID").cast("int").alias("pickup_zone"),
    field("DOLocationID").cast("int").alias("dropoff_zone"),
    field("trip_miles").cast("double").alias("trip_miles"),
    field("base_passenger_fare").cast(DecimalType(18,2)).alias("fare_usd"),
    field("hvfhs_license_num").cast("string").alias("provider_code"),
    (field("tips").cast(DecimalType(18,2)) if "tips" in names
     else F.lit(None).cast(DecimalType(18,2))).alias("tips_usd")
))
reasons = F.concat_ws("; ",
    F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "invalid trip time"),
    F.when(F.col("dropoff_ts") < F.col("pickup_ts"), "negative trip duration"),
    F.when(F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull(), "unknown zone id"),
    F.when(F.col("trip_miles").isNull() | (F.col("trip_miles") < 0), "invalid distance"),
    F.when(F.col("fare_usd").isNull() | (F.col("fare_usd") < 0), "invalid base fare"))
tagged = rides.withColumn("quality_issue", reasons)
invalid = tagged.filter(F.length("quality_issue") > 0)
valid = (tagged.filter(F.length("quality_issue") == 0)
         .withColumn("pickup_date", F.to_date("pickup_ts"))
         .withColumn("trip_minutes",
             (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60)
         .drop("quality_issue"))
invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_trip_quarantine")
valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_valid_trips")

# COMMAND ----------
# Zone is a dimension: two independent location keys allow pickup and dropoff
# exploration. No individual passenger or driver IDs are included in the Gold table.
z = (zone_csv.select(
    F.col("LocationID").cast("int").alias("zone_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone_name"))
    .dropDuplicates(["zone_id"]))
weather = (weather_json
    .select(F.explode(F.arrays_zip(
        F.col("daily.time"), F.col("daily.precipitation_sum"),
        F.col("daily.temperature_2m_max"), F.col("daily.temperature_2m_min"))).alias("w"))
    .select(F.to_date("w.time").alias("weather_date"),
            F.col("w.precipitation_sum").cast("double").alias("precipitation_mm"),
            F.col("w.temperature_2m_max").cast("double").alias("max_temp_c"),
            F.col("w.temperature_2m_min").cast("double").alias("min_temp_c")))
weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_weather")

enriched = (valid.alias("r")
    .join(F.broadcast(z).alias("z"), F.col("r.pickup_zone") == F.col("z.zone_id"), "left")
    .join(F.broadcast(weather).alias("w"), F.col("r.pickup_date") == F.col("w.weather_date"), "left")
    .select("r.*",
            F.coalesce(F.col("z.borough"), F.lit("UNKNOWN")).alias("pickup_borough"),
            F.coalesce(F.col("z.zone_name"), F.lit("UNKNOWN")).alias("pickup_zone_name"),
            "w.precipitation_mm", "w.max_temp_c", "w.min_temp_c"))
enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_enriched_trips")

# COMMAND ----------
# Gold aggregates real trips: true input row counts, not made-up throughput.
gold = (enriched.groupBy("pickup_date", "pickup_borough", "pickup_zone", "pickup_zone_name")
    .agg(F.count("*").alias("trip_count"),
         F.sum("fare_usd").alias("base_fare_usd"),
         F.sum("tips_usd").alias("tips_usd"),
         F.avg("trip_miles").alias("avg_trip_miles"),
         F.avg("trip_minutes").alias("avg_trip_minutes"),
         F.avg("precipitation_mm").alias("avg_daily_precipitation_mm"))
    .orderBy("pickup_date", "pickup_borough", "pickup_zone"))
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_zone_day_operations")

# COMMAND ----------
total = raw.count()
valid_n = valid.count()
quarantine_n = invalid.count()
assert valid_n + quarantine_n == total, "Bronze and Silver row reconciliation failed"
assert enriched.count() == valid_n, "Enrichment duplicated or lost trips"
assert gold.agg(F.sum("trip_count")).first()[0] == valid_n
assert gold.filter("trip_count < 1").count() == 0
assert gold.filter("pickup_zone IS NULL").count() == 0
print("PASS real TLC trip ingestion", {"bronze": total, "valid": valid_n,
                                       "quarantine": quarantine_n,
                                       "gold_groups": gold.count()})
display(spark.table("gold_zone_day_operations").orderBy(F.col("trip_count").desc()).limit(30))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evidence and limitations
# MAGIC Capture the actual month, source SHA256, Bronze/Gold counts and query profile.
# MAGIC TLC does not issue trip IDs in HVFHV Parquet, so identical-looking trips must
# MAGIC NOT be blindly dropped as duplicate events. This is batch analytical processing,
# MAGIC not Event Hubs or exactly-once streaming. Weather is NYC-wide reanalysis,
# MAGIC not a measured value at each pickup zone. Use only grouped results.

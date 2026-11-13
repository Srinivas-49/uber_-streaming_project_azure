# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select * from uberxxxxxx000000.bronze.rides_raw

# COMMAND ----------

files=[
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]
 
for i in files:
    file=i["file"]
    df=spark.sql(f"""
    select * from read_files('abfss://raw@uberadlsstorage0000.dfs.core.windows.net/igestion/{file}.json', format => 'json', multiLine => true)
    """)
    df.write.mode("overwrite").saveAsTable(f"uberxxxxxx000000.bronze.{file}")
    


# COMMAND ----------

# MAGIC %sql
# MAGIC create volume uberxxxxxx000000.bronze.landing

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/uberxxxxxx000000/bronze/landing/checkpoint")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uberxxxxxx000000.bronze.bulk_rides limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uberxxxxxx000000.bronze.rides_raw limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from uberxxxxxx000000.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uberxxxxxx000000.bronze.stg_rides limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table uberxxxxxx000000.bronze.stg_rides

# COMMAND ----------

df=spark.read.table("uberxxxxxx000000.bronze.map_cities")

# COMMAND ----------

from pyspark.sql.functions import lit
from datetime import datetime
df=df.withColumn("updated_at",lit(datetime.now()))

df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("uberxxxxxx000000.bronze.map_cities")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table uberxxxxxx000000.bronze.map_vehicle_makes set TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC alter table uberxxxxxx000000.bronze.map_vehicle_makes drop columns (updated_at)
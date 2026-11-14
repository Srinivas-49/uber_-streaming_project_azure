# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap the synthetic Uber workspace
# MAGIC Uses an existing writable Unity Catalog catalog. No Event Hubs service is provisioned.

# COMMAND ----------
import re
catalog = spark.sql("SELECT current_catalog() AS name").first()["name"]
if not re.fullmatch(r"[A-Za-z0-9_]+", catalog) or catalog == "hive_metastore":
    raise RuntimeError("Select a writable Unity Catalog catalog before running this demo")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`uber_portfolio` "
          "COMMENT 'Synthetic rides and CDC training; not customer data'")
try:
    spark.sql(f"ALTER SCHEMA `{catalog}`.`uber_portfolio` SET TAGS "
              "('data_classification' = 'synthetic', 'project' = 'uber_portfolio')")
except Exception as exc:
    print("WARNING: UC schema exists but tag permissions unavailable:", type(exc).__name__, str(exc)[:240])
spark.sql("""
CREATE TABLE IF NOT EXISTS uber_portfolio.ops_quality_audit (
  checked_at TIMESTAMP,
  test_name STRING,
  measured BIGINT,
  result STRING
) USING DELTA
""")
print("PASS: uber_portfolio schema and quality audit table prepared")

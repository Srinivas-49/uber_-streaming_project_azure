"""Pure PySpark data-quality utilities; importable outside Databricks."""
from pyspark.sql import DataFrame, functions as F

REQUIRED_RIDE_COLUMNS = ("ride_id", "booking_timestamp")

def validate_schema(df: DataFrame, required=REQUIRED_RIDE_COLUMNS) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

def quality_metrics(df: DataFrame) -> dict:
    """Compute duplicate IDs and null identifiers; use on bounded microbatches."""
    validate_schema(df)
    row = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("ride_id").isNull(), 1).otherwise(0)).alias("null_ride_ids"),
        F.countDistinct("ride_id").alias("distinct_ride_ids"),
    ).first()
    count = row["row_count"]
    nulls = row["null_ride_ids"] or 0
    distinct = row["distinct_ride_ids"]
    return {"row_count": count, "null_ride_ids": nulls,
            "duplicate_ride_ids": count - nulls - distinct}

def assert_ride_quality(df: DataFrame) -> dict:
    metrics = quality_metrics(df)
    if metrics["null_ride_ids"] or metrics["duplicate_ride_ids"]:
        raise ValueError(f"Ride quality checks failed: {metrics}")
    return metrics

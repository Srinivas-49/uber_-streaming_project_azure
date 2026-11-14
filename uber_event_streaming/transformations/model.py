"""Gold CDC tables. Use event/update timestamps instead of IDs as sequence columns.

Production must define deterministic tie-breaking for same-key equal timestamps,
proper deletion semantics and PII security grants before deployment.
"""
from pyspark import pipelines as dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("uber.catalog", "uberxxxxxx000000")
SILVER = f"{CATALOG}.silver.silver_obt"

def changes(columns, key, time_column="booking_timestamp"):
    return (spark.readStream.table(SILVER).select(*columns)
            .where(F.col(key).isNotNull() & F.col(time_column).isNotNull()))

@dlt.temporary_view(name="passenger_changes")
def passenger_changes():
    return changes(["passenger_id", "passenger_name", "passenger_phone", "passenger_email", "booking_timestamp"], "passenger_id")

dlt.create_streaming_table(f"{CATALOG}.gold.dim_passenger")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_passenger", source="passenger_changes", keys=["passenger_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

@dlt.temporary_view(name="driver_changes")
def driver_changes():
    return changes(["driver_id", "driver_name", "driver_phone", "driver_license", "driver_rating", "booking_timestamp"], "driver_id")

dlt.create_streaming_table(f"{CATALOG}.gold.dim_driver")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_driver", source="driver_changes", keys=["driver_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

@dlt.temporary_view(name="vehicle_changes")
def vehicle_changes():
    return changes(["vehicle_id", "vehicle_make_id", "vehicle_model", "vehicle_color", "vehicle_type_id", "license_plate", "booking_timestamp"], "vehicle_id")

dlt.create_streaming_table(f"{CATALOG}.gold.dim_vehicle")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_vehicle", source="vehicle_changes", keys=["vehicle_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

@dlt.temporary_view(name="payment_method_changes")
def payment_method_changes():
    return changes(["payment_method_id", "payment_method", "is_card", "requires_auth", "booking_timestamp"], "payment_method_id")

dlt.create_streaming_table(f"{CATALOG}.gold.dim_payment_method")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_payment_method", source="payment_method_changes", keys=["payment_method_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

@dlt.temporary_view(name="booking_changes")
def booking_changes():
    return changes(["ride_id", "confirmation_number", "dropoff_location_id", "ride_status_id", "dropoff_city_id", "cancellation_reason_id", "dropoff_address", "dropoff_latitude", "dropoff_longitude", "booking_timestamp", "dropoff_timestamp", "pickup_address", "pickup_latitude", "pickup_longitude", "pickup_location_id"], "ride_id")

dlt.create_streaming_table(f"{CATALOG}.gold.dim_bookings")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_bookings", source="booking_changes", keys=["ride_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

@dlt.temporary_view(name="location_changes")
def location_changes():
    return (spark.readStream.table(SILVER)
            .select("pickup_city_id", "pickup_city", "city_updated_at", "region", "state")
            .withColumn("city_updated_at", F.col("city_updated_at").cast("timestamp"))
            .where(F.col("pickup_city_id").isNotNull() & F.col("city_updated_at").isNotNull()))

dlt.create_streaming_table(f"{CATALOG}.gold.dim_location_scd2")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.dim_location_scd2", source="location_changes", keys=["pickup_city_id"], sequence_by=F.col("city_updated_at"), stored_as_scd_type=2)

@dlt.temporary_view(name="ride_fact_changes")
def ride_fact_changes():
    return changes(["ride_id", "pickup_city_id", "payment_method_id", "driver_id", "passenger_id", "vehicle_id", "distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute", "booking_timestamp"], "ride_id")

dlt.create_streaming_table(f"{CATALOG}.gold.fact_ride")
dlt.create_auto_cdc_flow(target=f"{CATALOG}.gold.fact_ride", source="ride_fact_changes", keys=["ride_id"], sequence_by=F.col("booking_timestamp"), stored_as_scd_type=1)

from pyspark import pipelines as dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.temporary_view(
    name="passenger_view"
)
def temp_view():
    df=spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df=df.select("passenger_id","passenger_name","passenger_phone","passenger_email","booking_timestamp")
    df=df.dropDuplicates(["passenger_id"])
    return df
dlt.create_streaming_table("uberxxxxxx000000.gold.dim_passenger")
dlt.create_auto_cdc_flow(
    target="uberxxxxxx000000.gold.dim_passenger",
    source="passenger_view",
    keys=['passenger_id'],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1
)

@dlt.temporary_view(
    name="driver_view"

)
def temp_view():
    df=spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df=df.select("driver_id","driver_name","driver_phone","driver_license","driver_rating")
    df=df.dropDuplicates(["driver_id"])
    return df

dlt.create_streaming_table("uberxxxxxx000000.gold.dim_driver")
dlt.create_auto_cdc_flow(
    target="uberxxxxxx000000.gold.dim_driver",
    source="driver_view",
    keys=['driver_id'],
    sequence_by="driver_id",
    stored_as_scd_type=1
)

@dlt.temporary_view(
    name="vechicle_view"
)
def temp_view():
    df=spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df=df.select("vehicle_id","vehicle_make_id","vehicle_model","vehicle_color","vehicle_type_id","license_plate")
    df=df.dropDuplicates(["vehicle_id"])
    return df

dlt.create_streaming_table("uberxxxxxx000000.gold.dim_vehicle")
dlt.create_auto_cdc_flow(
    target="uberxxxxxx000000.gold.dim_vehicle",
    source="vechicle_view",
    keys=['vehicle_id'],
    sequence_by="vehicle_id",
    stored_as_scd_type=1
)

@dlt.temporary_view(
    name="payment_method_view"
)
def temp_view():
    df=spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df=df.select("payment_method_id","payment_method","is_card","requires_auth")
    df=df.dropDuplicates(["payment_method_id"])
    return df

dlt.create_streaming_table("uberxxxxxx000000.gold.dim_payment_method")
dlt.create_auto_cdc_flow(
    target="uberxxxxxx000000.gold.dim_payment_method",
    source="payment_method_view",
    keys=['payment_method_id'],
    sequence_by="payment_method_id",
    stored_as_scd_type=1
)

@dlt.temporary_view(
    name="bookings_view"
)
def temp_view():
    df=spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df = df.select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_latitude","pickup_longitude","pickup_location_id")
    df=df.dropDuplicates(["ride_id"])
    return df

dlt.create_streaming_table("uberxxxxxx000000.gold.dim_bookings")
dlt.create_auto_cdc_flow(
    target="uberxxxxxx000000.gold.dim_bookings",
    source="bookings_view",
    keys=['ride_id'],
    sequence_by="ride_id"
)

@dlt.temporary_view()
def location_view():
    df = spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    df = df.select("pickup_city_id","pickup_city","city_updated_at","region","state",)
    df = df.dropDuplicates(subset=['pickup_city_id','city_updated_at'])
    return df

dlt.create_streaming_table("dim_location_scd2")
dlt.create_auto_cdc_flow(
  target = "dim_location_scd2",
  source = "location_view",
  keys = ["pickup_city_id"],
  sequence_by = "city_updated_at",
  stored_as_scd_type = 2,
)

@dlt.temporary_view()
def fact_view():
    df = spark.readStream.table("uberxxxxxx000000.silver.silver_obt")
    
    df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","total_fare","tip_amount","rating","base_rate","per_mile","per_minute")
    return df

dlt.create_streaming_table("fact")
dlt.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)

























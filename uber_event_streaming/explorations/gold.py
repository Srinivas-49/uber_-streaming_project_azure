# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Uber Rides - Gold Layer Explorations
# MAGIC Aggregations and charts over the gold dimension tables in `uberxxxxxx000000.gold` (`dim_bookings`, `dim_driver`, `dim_passenger_scd1`, `dim_vehicle`, `dim_payment_method`). Bronze `map_*` lookup tables are joined only to turn IDs into readable labels.

# COMMAND ----------

# DBTITLE 1,Setup: tables and chart style
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from pyspark.sql import functions as F

CAT = "uberxxxxxx000000"
bookings  = spark.table(f"{CAT}.gold.dim_bookings")
drivers   = spark.table(f"{CAT}.gold.dim_driver")
passengers= spark.table(f"{CAT}.gold.dim_passenger_scd1")
vehicles  = spark.table(f"{CAT}.gold.dim_vehicle")
payments  = spark.table(f"{CAT}.gold.dim_payment_method")

# Lookup tables used only to label codes
statuses = spark.table(f"{CAT}.bronze.map_ride_statuses").select("ride_status_id", "ride_status")
reasons  = spark.table(f"{CAT}.bronze.map_cancellation_reasons").select("cancellation_reason_id", F.coalesce("cancellation_reason", F.lit("Not specified")).alias("cancellation_reason"))
cities   = spark.table(f"{CAT}.bronze.map_cities").select("city_id", "city", "state", "region")
makes    = spark.table(f"{CAT}.bronze.map_vehicle_makes")
vtypes   = spark.table(f"{CAT}.bronze.map_vehicle_types").select("vehicle_type_id", "vehicle_type")

UBER = ["#000000", "#276EF1", "#05A357", "#FFC043", "#E11900", "#7356BF", "#757575", "#1E9AD6", "#FF6937", "#B7B7B7"]
plt.rcParams.update({
    "figure.figsize": (11, 5), "figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "axes.titlesize": 14, "axes.titleweight": "bold",
    "axes.labelsize": 11, "axes.prop_cycle": plt.cycler(color=UBER), "font.family": "DejaVu Sans",
})

# COMMAND ----------

# DBTITLE 1,KPI summary
kpi = (bookings.join(statuses, "ride_status_id", "left")
       .agg(F.countDistinct("ride_id").alias("total_bookings"),
            F.sum(F.when(F.col("ride_status") == "Completed", 1).otherwise(0)).alias("completed_rides"),
            F.sum(F.when(F.col("ride_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_rides"),
            F.min("booking_timestamp").alias("first_booking"), F.max("booking_timestamp").alias("last_booking"))
       .withColumn("cancellation_rate_pct", F.round(100 * F.col("cancelled_rides") / F.col("total_bookings"), 2))
       .crossJoin(drivers.agg(F.count("*").alias("drivers"), F.round(F.avg("driver_rating"), 2).alias("avg_driver_rating")))
       .crossJoin(passengers.agg(F.count("*").alias("passengers")))
       .crossJoin(vehicles.agg(F.count("*").alias("vehicles"))))
display(kpi)

# COMMAND ----------

# DBTITLE 1,Monthly booking trend by status
monthly = (bookings.join(statuses, "ride_status_id", "left")
           .groupBy(F.date_trunc("month", "booking_timestamp").alias("month"))
           .pivot("ride_status", ["Completed", "Cancelled"]).count().fillna(0)
           .orderBy("month"))
display(monthly)

pdf = monthly.toPandas()
fig, ax = plt.subplots()
ax.plot(pdf["month"], pdf["Completed"], marker="o", lw=2.5, label="Completed", color=UBER[2])
ax.plot(pdf["month"], pdf["Cancelled"], marker="o", lw=2.5, label="Cancelled", color=UBER[4])
ax.set(title="Monthly Bookings by Ride Status", xlabel="Month", ylabel="Bookings")
ax.legend(frameon=False)
fig.autofmt_xdate()
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Ride status and cancellation reasons
status_mix = bookings.join(statuses, "ride_status_id", "left").groupBy("ride_status").count().orderBy(F.desc("count"))
cancel_mix = (bookings.join(statuses, "ride_status_id").filter("ride_status = 'Cancelled'")
              .join(reasons, "cancellation_reason_id", "left")
              .groupBy("cancellation_reason").count().orderBy(F.desc("count")))
display(status_mix); display(cancel_mix)

s, c = status_mix.toPandas(), cancel_mix.toPandas()
fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))
a1.pie(s["count"], labels=s["ride_status"], autopct="%1.1f%%", startangle=90, colors=[UBER[2], UBER[4]],
       wedgeprops=dict(width=0.4, edgecolor="white"))
a1.set_title("Ride Status Mix")
b = a2.barh(c["cancellation_reason"], c["count"], color=UBER[1])
a2.invert_yaxis(); a2.set(title="Cancellation Reasons (Cancelled Rides)", xlabel="Rides")
a2.bar_label(b, padding=3, fontsize=9)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Bookings by dropoff city and region
city_agg = (bookings.join(cities, bookings.dropoff_city_id == cities.city_id, "left")
            .join(statuses, "ride_status_id", "left")
            .groupBy("city", "state", "region")
            .agg(F.count("*").alias("bookings"),
                 F.round(100 * F.avg(F.when(F.col("ride_status") == "Cancelled", 1).otherwise(0)), 2).alias("cancellation_rate_pct"))
            .orderBy(F.desc("bookings")))
display(city_agg)

p = city_agg.toPandas()
fig, ax = plt.subplots()
bars = ax.bar(p["city"], p["bookings"], color=UBER[0])
ax.bar_label(bars, padding=3, fontsize=9)
ax2 = ax.twinx(); ax2.plot(p["city"], p["cancellation_rate_pct"], color=UBER[4], marker="D", lw=2, label="Cancellation rate")
ax2.yaxis.set_major_formatter(mtick.PercentFormatter()); ax2.grid(False); ax2.set_ylabel("Cancellation rate")
ax.set(title="Bookings and Cancellation Rate by Dropoff City", ylabel="Bookings")
plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
ax2.legend(loc="upper right", frameon=False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Booking demand heatmap (day x hour)
heat = (bookings.groupBy(F.dayofweek("booking_timestamp").alias("dow"), F.hour("booking_timestamp").alias("hour"))
        .count())
hp = heat.toPandas().pivot(index="dow", columns="hour", values="count").reindex(index=range(1, 8), columns=range(24)).fillna(0)

fig, ax = plt.subplots(figsize=(13, 4.5))
im = ax.imshow(hp.values, aspect="auto", cmap="Blues")
ax.set_yticks(range(7), ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"])
ax.set_xticks(range(24), [f"{h:02d}" for h in range(24)])
ax.set(title="Booking Demand by Day of Week and Hour (UTC)", xlabel="Hour of day"); ax.grid(False)
fig.colorbar(im, ax=ax, label="Bookings")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Driver rating distribution
rating_bands = (drivers.withColumn("rating_band", F.format_number(F.floor(F.col("driver_rating") * 10) / 10, 1))
                .groupBy("rating_band").count().orderBy("rating_band"))
display(drivers.agg(F.count("*").alias("drivers"), F.round(F.avg("driver_rating"), 2).alias("avg"),
                    F.percentile_approx("driver_rating", 0.5).alias("median"),
                    F.min("driver_rating").alias("min"), F.max("driver_rating").alias("max")))

r = rating_bands.toPandas()
fig, ax = plt.subplots()
bars = ax.bar(r["rating_band"], r["count"], color=UBER[1])
ax.bar_label(bars, padding=3, fontsize=9)
ax.set(title="Driver Rating Distribution", xlabel="Rating band", ylabel="Drivers")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Vehicle fleet composition
# bronze map_vehicle_makes currently has null names, so fall back to the make id as the label
fleet_make = (vehicles.join(makes.filter("vehicle_make_id IS NOT NULL"), "vehicle_make_id", "left")
              .groupBy(F.coalesce("vehicle_make", F.concat(F.lit("Make "), F.col("vehicle_make_id").cast("string"))).alias("vehicle_make"))
              .count().orderBy(F.desc("count")))
fleet_type = vehicles.join(vtypes, "vehicle_type_id", "left").groupBy("vehicle_type").count().orderBy(F.desc("count"))
fleet_color = vehicles.groupBy("vehicle_color").count().orderBy(F.desc("count")).limit(10)
display(fleet_make); display(fleet_type)

m, t, c = (df.toPandas().fillna("Unknown") for df in (fleet_make, fleet_type, fleet_color))
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
for ax, df, col, title, color in [(axes[0], m, "vehicle_make", "Vehicles by Make", UBER[0]),
                                  (axes[1], t, "vehicle_type", "Vehicles by Type", UBER[1]),
                                  (axes[2], c, "vehicle_color", "Top 10 Vehicle Colors", UBER[5])]:
    b = ax.barh(df[col], df["count"], color=color)
    ax.invert_yaxis(); ax.bar_label(b, padding=3, fontsize=9); ax.set(title=title, xlabel="Vehicles")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Passenger activity by last booking month
# dim_passenger_scd1 keeps the latest booking_timestamp per passenger
pax = (passengers.groupBy(F.date_trunc("month", "booking_timestamp").alias("last_booking_month"))
       .agg(F.count("*").alias("passengers")).orderBy("last_booking_month"))
display(pax)

p = pax.toPandas()
fig, ax = plt.subplots()
ax.fill_between(p["last_booking_month"], p["passengers"], color=UBER[7], alpha=0.3)
ax.plot(p["last_booking_month"], p["passengers"], color=UBER[7], lw=2.5, marker="o")
ax.set(title="Passengers by Most Recent Booking Month", xlabel="Month", ylabel="Passengers")
fig.autofmt_xdate(); plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Payment method catalog
# dim_bookings does not carry payment_method_id yet, so this shows the payment dimension itself
display(payments.orderBy("payment_method_id"))
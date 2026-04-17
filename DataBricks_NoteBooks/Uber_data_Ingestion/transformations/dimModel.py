from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# For Dim passesnger

@dp.temporary_view()
def dim_Passenger_view():
       df=spark.readStream.table("silver_obt")
       df=df.select("passenger_id","passenger_name","passenger_email","passenger_phone")
       df=df.dropDuplicates(subset=['passenger_id'])
       return df

dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_Passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = 1
   
)

# For Dim Driver

@dp.temporary_view()
def dim_Driver_view():
       df=spark.readStream.table("silver_obt")
       df=df.select("driver_id","driver_name","driver_rating","driver_phone","driver_license")
       df=df.dropDuplicates(subset=['driver_id'])
       return df

dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_Driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = 1
   
)


# For Dim Vehicle



@dp.temporary_view()
def dim_Vehicle_view():
       df=spark.readStream.table("silver_obt")
       df=df.select("vehicle_id","vehicle_type_id","vehicle_make_id","vehicle_model","vehicle_color","license_plate","vehicle_make","vehicle_type")
       df=df.dropDuplicates(subset=['vehicle_id'])
       return df

dp.create_streaming_table("dim_Vehicle")

dp.create_auto_cdc_flow(
  target = "dim_Vehicle",
  source = "dim_Vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = 1
   
)


# Dim Payment

@dp.temporary_view()
def dim_payment_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("payment_method_id")
    df = df.dropDuplicates(subset=['payment_method_id'])
    return df

dp.create_streaming_table("dim_payment")
dp.create_auto_cdc_flow(
    target = "dim_payment",
    source = "dim_payment_view",
    keys = ["payment_method_id"],
    sequence_by = "payment_method_id",
    stored_as_scd_type = 1,
)


# Master table or master data which contain the important information
# Dim Booking
@dp.temporary_view()
def dim_booking_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(
        "ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_latitude","pickup_longitude","pickup_location_id"
    )
    df = df.dropDuplicates(subset=['ride_id'])
    return df

dp.create_streaming_table("dim_booking")
dp.create_auto_cdc_flow(
    target = "dim_booking",
    source = "dim_booking_view", 
    keys = ["ride_id"],
    sequence_by = "ride_id",
    stored_as_scd_type = 1,
)

# DIM LOCATION

@dp.table
def dim_location_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("pickup_city_id", "pickup_city", "city_updated_at", "pickup_region", "state")
    df = df.dropDuplicates(subset=['pickup_city_id','city_ucity_updated_atpdated_at'])
    return df

dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
    target = "dim_location",
    source = "dim_location_view",
    keys = ["pickup_city_id"],
    sequence_by = "city_updated_at",
    stored_as_scd_type = 2
)

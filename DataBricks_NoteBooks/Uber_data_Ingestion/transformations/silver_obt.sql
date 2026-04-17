create or refresh streaming table silver_obt
as 

           select 
           
               stg_rides.ride_id,stg_rides.confirmation_number,stg_rides.passenger_id,stg_rides.driver_id,stg_rides.vehicle_id,stg_rides.pickup_location_id,stg_rides.dropoff_location_id,stg_rides.vehicle_type_id,stg_rides.vehicle_make_id,stg_rides.payment_method_id,stg_rides.ride_status_id,stg_rides.pickup_city_id,stg_rides.dropoff_city_id,stg_rides.cancellation_reason_id,stg_rides.passenger_name,stg_rides.passenger_email,stg_rides.passenger_phone,stg_rides.driver_name,stg_rides.driver_rating,stg_rides.driver_phone,stg_rides.driver_license,stg_rides.vehicle_model,stg_rides.vehicle_color,stg_rides.license_plate,stg_rides.pickup_address,stg_rides.pickup_latitude,stg_rides.pickup_longitude,stg_rides.dropoff_address,stg_rides.dropoff_latitude,stg_rides.dropoff_longitude,stg_rides.distance_miles,stg_rides.duration_minutes,stg_rides.booking_timestamp,stg_rides.pickup_timestamp,stg_rides.dropoff_timestamp,stg_rides.base_fare,stg_rides.distance_fare,stg_rides.time_fare,stg_rides.surge_multiplier,stg_rides.subtotal,stg_rides.tip_amount,stg_rides.total_fare,stg_rides.rating
                  
                          ,
                   
           
             map_vehicle_makes.vehicle_make
                  
                          ,
                   
           
             map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute
                              ,

           

             map_cities.city as pickup_city,
             map_cities.region as pickup_region,
             map_cities.state as pickup_state,
             map_cities.updated_at as pickup_city_updated_at
                  
           

        from 
           
              
                stream (uber_project.bronze_layer.stg_rides)
                watermark booking_timestamp delay of interval 3 minutes stg_rides
                  
                  
              
                  left join uber_project.bronze_layer.map_vechile_makes map_vehicle_makes on stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
                  
                  
              
                  left join uber_project.bronze_layer.map_vechile_types map_vehicle_types on stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id

                   left join uber_project.bronze_layer.map_cities map_cities on stg_rides.pickup_city_id = map_cities.city_id


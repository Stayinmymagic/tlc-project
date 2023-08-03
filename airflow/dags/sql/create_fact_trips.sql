CREATE OR REPLACE TABLE `core.fact_trips` AS (
  with 
    green_data as (
      select * , 'Green' as service_type from `staging.green_view`),
    yellow_data as (
      select * , 'Yellow' as service_type from `staging.yellow_view`),
    trips_unioned as (
      select * from green_data
      union all 
      select * from yellow_data),
    dim_zones as (select * from `staging.zone_external_table` where borough != 'Unknown')
    
  select 
    trips_unioned.tripid,
    -- timestamps  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    -- trip info 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    pickup_zone.lat as pickup_lat,
    pickup_zone.long as pickup_long,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.lat as dropoff_lat, 
    dropoff_zone.long as dropoff_long,
    trips_unioned.trip_distance, 
    -- payment info
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.improvement_sucharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type_description, 
    trips_unioned.congestion_surcharge
  from trips_unioned
  inner join dim_zones as pickup_zone
  on trips_unioned.pickup_locationid = pickup_zone.locationid
  inner join dim_zones as dropoff_zone
  on trips_unioned.dropoff_locationid = dropoff_zone.locationid
  
)
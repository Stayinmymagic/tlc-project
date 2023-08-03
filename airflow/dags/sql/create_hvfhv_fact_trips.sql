CREATE OR REPLACE TABLE `core.fhvhv_fact_table` AS
  WITH
    fhvhv_data as (
      select * from `staging.fhvhv_data`),
    dim_zones as (
      select * from `staging.zone_external_table` where borough != 'Unknown')

  SELECT 
      fhvhv_data.tripid,
      -- company info 
      fhvhv_data.driver_company,
      fhvhv_data.driver_license_number as driver,
      -- timestamps
      fhvhv_data.request_datetime,
      fhvhv_data.pickup_datetime,
      -- trip info
      pickup_zone.borough as pickup_borough,
      pickup_zone.zone as pickup_zone,
      pickup_zone.lat as pickup_lat,
      pickup_zone.long as pickup_long,
      dropoff_zone.borough as dropoff_borough, 
      dropoff_zone.zone as dropoff_zone,
      dropoff_zone.lat as dropoff_lat, 
      dropoff_zone.long as dropoff_long,
      fhvhv_data.trip_miles,
      fhvhv_data.trip_time_mins,
      -- payment info
      fhvhv_data.base_passenger_fare as base_passenger_fare,
      fhvhv_data.tolls_fee as tolls_fee,
      fhvhv_data.bcf_fee as bcf_fee,
      fhvhv_data.nyc_sales_tax as nyc_sales_tax,
      fhvhv_data.congestion_surcharge as congestion_surcharge,
      fhvhv_data.tips_fee as tips_fee,
      -- base_passenger_fare+tolls_fee+bcf_fee+nyc_sales_tax+congestion_surcharge+tips_fee as total_fee,
      -- (tips_fee/total_fee) * 100 as percent_of_tips
  from fhvhv_data
  inner join dim_zones as pickup_zone
  on fhvhv_data.pickup_locationid = pickup_zone.locationid
  inner join dim_zones as dropoff_zone
  on fhvhv_data.dropoff_locationid = dropoff_zone.locationid;

Alter table core.fhvhv_fact_table
add COLUMN total_fee NUMERIC;

UPDATE  `core.fhvhv_fact_table`
set total_fee = base_passenger_fare+ 
      tolls_fee+
      bcf_fee+
      nyc_sales_tax+
      congestion_surcharge+
      tips_fee
where true;
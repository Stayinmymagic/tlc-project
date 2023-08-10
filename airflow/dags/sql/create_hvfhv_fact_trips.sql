CREATE OR REPLACE TABLE `core.HVfhv_fact_table` AS
  WITH
    HVfhv_data as (
      select * from `staging.HVfhv_view`),
    dim_zones as (
      select * from `staging.zone_external_table` where borough != 'Unknown')

  SELECT 
      HVfhv_data.tripid,
      -- company info 
      HVfhv_data.driver_company,
      HVfhv_data.driver_license_number as driver,
      -- timestamps
      HVfhv_data.request_datetime,
      HVfhv_data.pickup_datetime,
      -- trip info
      pickup_zone.borough as pickup_borough,
      pickup_zone.zone as pickup_zone,
      pickup_zone.lat as pickup_lat,
      pickup_zone.long as pickup_long,
      dropoff_zone.borough as dropoff_borough, 
      dropoff_zone.zone as dropoff_zone,
      dropoff_zone.lat as dropoff_lat, 
      dropoff_zone.long as dropoff_long,
      HVfhv_data.trip_distance,
      HVfhv_data.trip_time_mins,
      -- payment info
      HVfhv_data.base_passenger_fare as base_passenger_fare,
      HVfhv_data.tolls_fee as tolls_fee,
      HVfhv_data.bcf_fee as bcf_fee,
      HVfhv_data.nyc_sales_tax as nyc_sales_tax,
      HVfhv_data.congestion_surcharge as congestion_surcharge,
      HVfhv_data.tips_fee as tips_fee,
      -- base_passenger_fare+tolls_fee+bcf_fee+nyc_sales_tax+congestion_surcharge+tips_fee as total_fee,
      -- (tips_fee/total_fee) * 100 as percent_of_tips
  from HVfhv_data
  inner join dim_zones as pickup_zone
  on HVfhv_data.pickup_locationid = pickup_zone.locationid
  inner join dim_zones as dropoff_zone
  on HVfhv_data.dropoff_locationid = dropoff_zone.locationid;

Alter table core.HVfhv_fact_table
add COLUMN total_fee NUMERIC;

UPDATE  `core.HVfhv_fact_table`
set total_fee = base_passenger_fare+ 
      tolls_fee+
      bcf_fee+
      nyc_sales_tax+
      congestion_surcharge+
      tips_fee
where true;

Alter table core.HVfhv_fact_table
add COLUMN revenue_per_mile NUMERIC;

UPDATE  `core.HVfhv_fact_table`
set revenue_per_mile = round((total_fee / trip_distance),3)
where true;
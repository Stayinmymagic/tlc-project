create or replace table `core.dm_monthly_zone_summary` as
(
  with 
    tripdata as (
      select * from `core.fact_trips`)
  
  select 
  -- revenue grouping
  date_trunc(pickup_datetime, month) as revenue_month,
  pickup_zone as revenue_zone,

  -- revenue calculation
  sum(fare_amount) as sum_revenue_monthly_fare,
  sum(tip_amount) as sum_revenue_monthly_tip_amount,
  sum(total_amount) as sum_revenue_monthly_total_amount,
  sum(congestion_surcharge) as sum_revenue_monthly_congestion_surcharge,

  -- additional calculation
  count(tripid) as total_monthly_trips,
  avg(trip_distance) as avg_monthly_trip_distance,
  avg(total_amount/trip_distance) as avg_revenue_per_mile,
  avg(date_diff(dropoff_datetime, pickup_datetime, MINUTE)) as avg_trips_duration

  from tripdata
  group by 1,2
)
CREATE OR REPLACE TABLE `staging.HVfhv_tripdata`
PARTITION BY DATE(pickup_datetime) AS
  SELECT 
    concat(dispatching_base_num, request_datetime) as tripid,
    case hvfhs_license_num
        when 'HV0002' then 'Juno'
        when 'HV0003' then 'Uber'
        when 'HV0004' then 'Via'
        when 'HV0005' then 'Lyft'
    end as driver_company,
    dispatching_base_num as driver_license_number,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(request_datetime as timestamp) as request_datetime,
    cast(on_scene_datetime as timestamp) as on_scene_datetime,
    cast(pickup_datetime as timestamp) as pickup_datetime,

    -- trip info 
    cast(trip_miles as numeric) as trip_miles,
    cast(trip_time/60 as numeric) as trip_time_mins,
    cast(base_passenger_fare as numeric) as  base_passenger_fare,
    cast(tolls as numeric) as tolls_fee,
    cast(bcf as numeric) as bcf_fee,
    cast(sales_tax as numeric) as nyc_sales_tax,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    cast(tips as numeric) as tips_fee,
    cast(driver_pay as numeric) as driver_revenue,
    case shared_request_flag
        when 'Y' then 1
        when 'N' then 0
    end as shared_request_flag,
    case shared_match_flag
        when 'Y' then 1
        when 'N' then 0
    end as shared_match_flag,
    case access_a_ride_flag
        when 'Y' then 1
        when 'N' then 0
    end as access_a_ride_flag,
    case wav_request_flag
        when 'Y' then 1
        when 'N' then 0
    end as wav_request_flag,
    case wav_match_flag
        when 'Y' then 1
        when 'N' then 0
    end as wav_match_flag
  
  FROM `trips_data_all.HVfhv_external_table`
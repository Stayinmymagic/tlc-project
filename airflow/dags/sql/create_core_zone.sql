CREATE OR REPLACE TABLE `core.dim_zones` AS
SELECT 
    cast(locationID as integer) as locationid,
    cast(borough as string) as borough, 
    cast(zone as string) as zone, 
    cast(replace(service_zone, 'Boro', 'Green') as string) as service_zone,
    cast(lat as numeric) as lat,
    cast(long as numeric) as long,
    ST_GEOGPOINT(long, lat) as geopoint
    
FROM `staging.zone_external_table`

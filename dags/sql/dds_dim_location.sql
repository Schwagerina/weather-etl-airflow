MERGE INTO dds.dim_location AS dds
USING stg.city_csv AS stg
ON dds.geo_longitude = stg.geo_lon
   AND dds.geo_latitude = stg.geo_lat
WHEN NOT MATCHED THEN
    INSERT (geo_latitude, geo_longitude, city_name, region_name, country_name, population_cnt, etl_proccess_dttm)
    VALUES (stg.geo_lat, stg.geo_lon, stg.city, stg.region, stg.country, stg.population, CURRENT_TIMESTAMP)
WHEN MATCHED THEN
    UPDATE SET city_name         = stg.city,
               region_name       = stg.region,
               country_name      = stg.country,
               population_cnt    = stg.population,
               etl_proccess_dttm = CURRENT_TIMESTAMP;
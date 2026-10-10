DELETE FROM dds.fct_weather_hourly
WHERE report_dttm::DATE BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                            AND '{{ ds }}'::DATE;

INSERT INTO dds.fct_weather_hourly
    (location_id, report_dttm, temperature, dew_point, humidity, precipitation,
     wind_direction, wind_speed, peak_wind_speed, air_pressure,
     weather_condition_id, etl_proccess_dttm)
SELECT
    dim_loc.location_id,
    stg.report_dttm,
    (stg.raw_data::JSONB ->> 'temperature_2m')::FLOAT             AS temperature,
    (stg.raw_data::JSONB ->> 'dew_point_2m')::FLOAT               AS dew_point,
    (stg.raw_data::JSONB ->> 'relative_humidity_2m')::FLOAT::INT  AS humidity,
    (stg.raw_data::JSONB ->> 'precipitation')::FLOAT              AS precipitation,
    (stg.raw_data::JSONB ->> 'wind_direction_10m')::FLOAT::INT    AS wind_direction,
    (stg.raw_data::JSONB ->> 'wind_speed_10m')::FLOAT             AS wind_speed,
    (stg.raw_data::JSONB ->> 'wind_gusts_10m')::FLOAT             AS peak_wind_speed,
    (stg.raw_data::JSONB ->> 'surface_pressure')::FLOAT           AS air_pressure,
    (stg.raw_data::JSONB ->> 'weather_code')::FLOAT::INT          AS weather_condition_id,
    CURRENT_TIMESTAMP                                             AS etl_proccess_dttm
FROM stg.external_weather_api AS stg
JOIN dds.dim_location AS dim_loc
    ON stg.lat = dim_loc.geo_latitude
   AND stg.lon = dim_loc.geo_longitude
WHERE stg.report_dttm::DATE BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                                AND '{{ ds }}'::DATE
ON CONFLICT (location_id, report_dttm) DO UPDATE
SET temperature          = EXCLUDED.temperature,
    dew_point            = EXCLUDED.dew_point,
    humidity             = EXCLUDED.humidity,
    precipitation        = EXCLUDED.precipitation,
    wind_direction       = EXCLUDED.wind_direction,
    wind_speed           = EXCLUDED.wind_speed,
    peak_wind_speed      = EXCLUDED.peak_wind_speed,
    air_pressure         = EXCLUDED.air_pressure,
    weather_condition_id = EXCLUDED.weather_condition_id,
    etl_proccess_dttm    = CURRENT_TIMESTAMP;
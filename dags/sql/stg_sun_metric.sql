INSERT INTO stg.external_sun_api
    (lat, lon, report_dt, day_length_sec, sunrise, sunset, raw_hourly_uv, etl_proccess_dttm)
VALUES (%s, %s, %s, %s, %s, %s, %s, current_timestamp)
ON CONFLICT (lat, lon, report_dt) DO UPDATE
SET day_length_sec    = EXCLUDED.day_length_sec,
    sunrise           = EXCLUDED.sunrise,
    sunset            = EXCLUDED.sunset,
    raw_hourly_uv     = EXCLUDED.raw_hourly_uv,
    etl_proccess_dttm = current_timestamp;
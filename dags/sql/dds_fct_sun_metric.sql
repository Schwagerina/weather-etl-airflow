DELETE FROM dds.fct_sun_metric
WHERE report_dt BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                    AND '{{ ds }}'::DATE;

INSERT INTO dds.fct_sun_metric
    (location_id, report_dt, day_length_min, avg_uv_index, etl_proccess_dttm)
SELECT
    dl.location_id,
    s.report_dt,
    ROUND(s.day_length_sec / 60.0)::INT AS day_length_min,
    (
        SELECT AVG((uv->>'uv_index')::FLOAT)
        FROM json_array_elements(s.raw_hourly_uv::json) AS uv
        WHERE uv->>'uv_index' IS NOT NULL
    ) AS avg_uv_index,
    CURRENT_TIMESTAMP
FROM stg.external_sun_api s
JOIN dds.dim_location dl
  ON s.lat = dl.geo_latitude
 AND s.lon = dl.geo_longitude
WHERE s.report_dt BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                      AND '{{ ds }}'::DATE
ON CONFLICT (location_id, report_dt) DO UPDATE
SET day_length_min    = EXCLUDED.day_length_min,
    avg_uv_index      = EXCLUDED.avg_uv_index,
    etl_proccess_dttm = CURRENT_TIMESTAMP;
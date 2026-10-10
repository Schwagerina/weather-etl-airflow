-- 1. Удаляем окно [ds-3, ds] по всем срезам
DELETE FROM cdm.agg_weather
WHERE report_dt BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                    AND '{{ ds }}'::DATE;

-- 2. Дневные агрегаты
WITH daily AS (
    SELECT
        wh.report_dttm::DATE                AS report_dt,
        wh.location_id,
        MAX(wh.temperature)                 AS max_temperature,
        MIN(wh.temperature)                 AS min_temperature,
        AVG(wh.temperature)                 AS avg_temperature,
        AVG(wh.humidity)                    AS avg_humidity,
        AVG(wh.wind_speed)                  AS avg_wind_speed,
        MAX(wh.air_pressure)                AS max_air_pressure,
        MIN(wh.air_pressure)                AS min_air_pressure,
        SUM(wh.precipitation)               AS precipitation,
        -- признаки «дня с высокой влажностью/грозой»
        CASE WHEN AVG(wh.humidity) > 75 THEN 1 ELSE 0 END AS is_high_humidity_day,
        CASE WHEN MAX(
                 CASE WHEN wh.weather_condition_id IN (95, 96, 99) THEN 1 ELSE 0 END
             ) = 1 THEN 1 ELSE 0 END AS is_thunderstorm_day
    FROM dds.fct_weather_hourly wh
    WHERE wh.report_dttm::DATE BETWEEN '{{ ds }}'::DATE - INTERVAL '3 day'
                                  AND '{{ ds }}'::DATE
    GROUP BY 1, 2
),
-- 3. Присоединяем солнце/UV (одна строка на город-день)
daily_enriched AS (
    SELECT
        d.*,
        sm.day_length_min,
        sm.avg_uv_index
    FROM daily d
    LEFT JOIN dds.fct_sun_metric sm
           ON sm.location_id = d.location_id
          AND sm.report_dt   = d.report_dt
)
-- 4. Разворачиваем в три среза и вставляем
INSERT INTO cdm.agg_weather
    (scale_name, report_dt, city_name, region_name,
     max_temperature, min_temperature, avg_temperature,
     avg_humidity, avg_wind_speed, max_air_pressure, min_air_pressure,
     day_length_min, avg_uv_index,
     high_humidity_day_cnt, precipitation, thunderstorm_day_cnt,
     etl_proccess_dttm)
SELECT
    scale_name,
    period_dt,
    dl.city_name,
    dl.region_name,
    MAX(max_temperature),
    MIN(min_temperature),
    AVG(avg_temperature),
    AVG(avg_humidity)::INT,
    AVG(avg_wind_speed),
    MAX(max_air_pressure),
    MIN(min_air_pressure),
    AVG(day_length_min)::INT,
    AVG(avg_uv_index),
    SUM(is_high_humidity_day)::INT,
    SUM(precipitation),
    SUM(is_thunderstorm_day)::INT,
    CURRENT_TIMESTAMP
FROM (
    -- day
    SELECT 'day'   AS scale_name, report_dt   AS period_dt, * FROM daily_enriched
    UNION ALL
    -- week
    SELECT 'week'  AS scale_name, date_trunc('week',  report_dt)::DATE AS period_dt, * FROM daily_enriched
    UNION ALL
    -- month
    SELECT 'month' AS scale_name, date_trunc('month', report_dt)::DATE AS period_dt, * FROM daily_enriched
) t
JOIN dds.dim_location dl USING (location_id)
GROUP BY scale_name, period_dt, dl.city_name, dl.region_name;
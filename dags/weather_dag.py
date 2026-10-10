import datetime as dt
import json

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from lib import external_api

REF_CITY_URL = 'ref_city_url'
POSTGRES_CONN_ID = 'pg_conn'
DAYS_AGO = 3


# ---------- Task 1: справочник городов из CSV ----------
def get_city_csv_data():
    df = pd.read_csv(Variable.get(REF_CITY_URL))
    df.columns = df.columns.str.lower()

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('city_csv', engine, index=False,
              if_exists='replace', schema='stg')


# ---------- Task 3: загрузка погоды из API в stg ----------
def _execute_pg(pg_conn_id, sql, records):
    hook = PostgresHook(pg_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as con:
        con.execute(sql, records)


def get_weather_data(date, ti):
    city_coordinates = ti.xcom_pull(task_ids='stg_city_list')
    end_date = dt.date.fromisoformat(date)
    start_date = end_date - dt.timedelta(days=DAYS_AGO)

    records = []
    for coords in city_coordinates:
        lat, lon = float(coords[1]), float(coords[2])
        raw = external_api.get_point_weather(
            [lat, lon], [str(start_date), str(end_date)],
            external_api.Scale.HOURLY,
        )
        for rec in external_api.flatten_response(raw, 'hourly'):
            records.append((lat, lon, rec['time'], json.dumps(rec)))

    _execute_pg(
        POSTGRES_CONN_ID,
        '''
        INSERT INTO stg.external_weather_api(lat, lon, report_dttm, raw_data, etl_proccess_dttm)
        VALUES (%s, %s, %s, %s, current_timestamp)
        ON CONFLICT (lat, lon, report_dttm) DO UPDATE
        SET raw_data = EXCLUDED.raw_data,
            etl_proccess_dttm = current_timestamp;
        ''',
        records,
    )
    print(f'[stg_external_api] inserted {len(records)} records')


# ---------- Task 4 (новая): солнце и UV ----------
def get_sun_metric_data(date, ti):
    city_coordinates = ti.xcom_pull(task_ids='stg_city_list')
    end_date = dt.date.fromisoformat(date)
    start_date = end_date - dt.timedelta(days=DAYS_AGO)

    records = []
    for coords in city_coordinates:
        lat, lon = float(coords[1]), float(coords[2])
        raw = external_api.get_sun_metric(
            [lat, lon], [str(start_date), str(end_date)],
        )
        # hourly часть -> массив объектов {'time': ..., 'uv_index': ...}
        hourly_uv = external_api.flatten_response(raw, 'hourly')

        daily = raw.get('daily', {})
        for i, d in enumerate(daily.get('time', [])):
            records.append((
                lat, lon, d,
                daily['daylight_duration'][i],
                daily['sunrise'][i],
                daily['sunset'][i],
                json.dumps(hourly_uv),
            ))

    _execute_pg(
        POSTGRES_CONN_ID,
        '''
        INSERT INTO stg.external_sun_api
            (lat, lon, report_dt, day_length_sec, sunrise, sunset, raw_hourly_uv, etl_proccess_dttm)
        VALUES (%s, %s, %s, %s, %s, %s, %s, current_timestamp)
        ON CONFLICT (lat, lon, report_dt) DO UPDATE
        SET day_length_sec    = EXCLUDED.day_length_sec,
            sunrise           = EXCLUDED.sunrise,
            sunset            = EXCLUDED.sunset,
            raw_hourly_uv     = EXCLUDED.raw_hourly_uv,
            etl_proccess_dttm = current_timestamp;
        ''',
        records,
    )
    print(f'[stg_sun_api] inserted {len(records)} records')


# ---------- DAG ----------
args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
        dag_id='weather_dag',
        schedule='@daily',
        catchup=False,
        default_args=args,
) as dag:

    stg_city_csv_data = PythonOperator(
        task_id='get_city_csv_data',
        python_callable=get_city_csv_data,
    )

    stg_city_list = PostgresOperator(
        task_id='stg_city_list',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/stg_city_list_w_coords.sql',
        do_xcom_push=True,
        return_last=False,
    )

    stg_external_api = PythonOperator(
        task_id='stg_external_api',
        python_callable=get_weather_data,
        op_kwargs={'date': '{{ ds }}'},
    )

    stg_sun_api = PythonOperator(
        task_id='stg_sun_api',
        python_callable=get_sun_metric_data,
        op_kwargs={'date': '{{ ds }}'},
    )

    dds_dim_location = PostgresOperator(
        task_id='dds_dim_location',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/dds_dim_location.sql',
    )

    dds_fct_weather_hourly = PostgresOperator(
        task_id='dds_fct_weather_hourly',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/dds_fct_weather_hourly.sql',
    )

    dds_fct_sun_metric = PostgresOperator(
        task_id='dds_fct_sun_metric',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/dds_fct_sun_metric.sql',
    )

    cdm_agg_weather = PostgresOperator(
        task_id='cdm_agg_weather',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/cdm_agg_weather.sql',
    )

    # ---------- Граф ----------
    stg_city_csv_data >> stg_city_list >> [stg_external_api, stg_sun_api]
    stg_city_csv_data >> dds_dim_location
    [stg_external_api, dds_dim_location] >> dds_fct_weather_hourly
    [stg_sun_api,      dds_dim_location] >> dds_fct_sun_metric
    [dds_fct_weather_hourly, dds_fct_sun_metric] >> cdm_agg_weather
# 🌤️ Weather ETL Pipeline on Apache Airflow

**ETL-пайплайн для сбора, трансформации и агрегации погодных данных с трёхслойной архитектурой DWH (STG → DDS → CDM).**

[![Airflow](https://img.shields.io/badge/Airflow-2.6.3-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8-3776AB?logo=python)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker)](https://www.docker.com/)

---

## 📌 О проекте

Проект реализует полный ETL-цикл: от загрузки сырых данных из внешних API до построения аналитических витрин. Демонстрирует ключевые практики Data Engineering:

- **Инкрементальная загрузка** с окном `[ds-3, ds]` на основе логической даты Airflow
- **Идемпотентность** через `MERGE`, `ON CONFLICT DO UPDATE`, `DELETE + INSERT`
- **Слоистая архитектура DWH**: STG (сырые данные) → DDS (нормализованные факты и измерения) → CDM (агрегаты для BI)
- **Параллельное выполнение** независимых задач в DAG
- **Три среза витрины** (day / week / month) через `UNION ALL` из одного CTE

## 🏗️ Архитектура

```mermaid
graph TD
    A[GitHub: city.csv] -->|PythonOperator + pandas| B[stg.city_csv]
    C[Open-Meteo: Weather API] -->|PythonOperator + requests| D[stg.external_weather_api]
    E[Open-Meteo: Sun/UV API] -->|PythonOperator + requests| F[stg.external_sun_api]

    B -->|MERGE| G[dds.dim_location]
    B -->|flatten JSON| D
    B -->|flatten JSON| F

    G --> H[dds.fct_weather_hourly]
    D --> H
    G --> I[dds.fct_sun_metric]
    F --> I

    H --> J[cdm.agg_weather]
    I --> J

    style A fill:#f9f,stroke:#333
    style C fill:#f9f,stroke:#333
    style E fill:#f9f,stroke:#333
    style B fill:#bbf,stroke:#333
    style D fill:#bbf,stroke:#333
    style F fill:#bbf,stroke:#333
    style G fill:#bfb,stroke:#333
    style H fill:#bfb,stroke:#333
    style I fill:#bfb,stroke:#333
    style J fill:#fbb,stroke:#333
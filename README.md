\# 🌤️ Weather ETL Pipeline on Apache Airflow



\*\*ETL-пайплайн для сбора, трансформации и агрегации погодных данных с трёхслойной архитектурой DWH (STG → DDS → CDM).\*\*



\[!\[Airflow](https://img.shields.io/badge/Airflow-2.6.3-017CEE?logo=apacheairflow)](https://airflow.apache.org/)

\[!\[Python](https://img.shields.io/badge/Python-3.8-3776AB?logo=python)](https://www.python.org/)

\[!\[PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?logo=postgresql)](https://www.postgresql.org/)

\[!\[Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker)](https://www.docker.com/)



\---



\## 📌 О проекте



Проект реализует полный ETL-цикл: от загрузки сырых данных из внешних API до построения аналитических витрин. Демонстрирует ключевые практики Data Engineering:



\- \*\*Инкрементальная загрузка\*\* с окном `\[ds-3, ds]` на основе логической даты Airflow

\- \*\*Идемпотентность\*\* через `MERGE`, `ON CONFLICT DO UPDATE`, `DELETE + INSERT`

\- \*\*Слоистая архитектура DWH\*\*: STG (сырые данные) → DDS (нормализованные факты и измерения) → CDM (агрегаты для BI)

\- \*\*Параллельное выполнение\*\* независимых задач в DAG

\- \*\*Три среза витрины\*\* (day / week / month) через `UNION ALL` из одного CTE



\## 🏗️ Архитектура



```mermaid

graph TD

&#x20;   A\[GitHub: city.csv] -->|PythonOperator + pandas| B\[stg.city\_csv]

&#x20;   C\[Open-Meteo: Weather API] -->|PythonOperator + requests| D\[stg.external\_weather\_api]

&#x20;   E\[Open-Meteo: Sun/UV API] -->|PythonOperator + requests| F\[stg.external\_sun\_api]



&#x20;   B -->|MERGE| G\[dds.dim\_location]

&#x20;   B -->|flatten JSON| D

&#x20;   B -->|flatten JSON| F



&#x20;   G --> H\[dds.fct\_weather\_hourly]

&#x20;   D --> H

&#x20;   G --> I\[dds.fct\_sun\_metric]

&#x20;   F --> I



&#x20;   H --> J\[cdm.agg\_weather]

&#x20;   I --> J



&#x20;   style A fill:#f9f,stroke:#333

&#x20;   style C fill:#f9f,stroke:#333

&#x20;   style E fill:#f9f,stroke:#333

&#x20;   style B fill:#bbf,stroke:#333

&#x20;   style D fill:#bbf,stroke:#333

&#x20;   style F fill:#bbf,stroke:#333

&#x20;   style G fill:#bfb,stroke:#333

&#x20;   style H fill:#bfb,stroke:#333

&#x20;   style I fill:#bfb,stroke:#333

&#x20;   style J fill:#fbb,stroke:#333


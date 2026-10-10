### \# 🌤️ Weather ETL Pipeline on Apache Airflow

### 

### \*\*ETL-пайплайн для сбора, трансформации и агрегации погодных данных с трёхслойной архитектурой DWH (STG → DDS → CDM).\*\*

### 

### \[!\[Airflow](https://img.shields.io/badge/Airflow-2.6.3-017CEE?logo=apacheairflow)](https://airflow.apache.org/)

### \[!\[Python](https://img.shields.io/badge/Python-3.8-3776AB?logo=python)](https://www.python.org/)

### \[!\[PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?logo=postgresql)](https://www.postgresql.org/)

### \[!\[Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker)](https://www.docker.com/)

### 

### \---

### 

### \## 📌 О проекте

### 

### Проект реализует полный ETL-цикл: от загрузки сырых данных из внешних API до построения аналитических витрин. Демонстрирует ключевые практики Data Engineering:

### 

### \- \*\*Инкрементальная загрузка\*\* с окном `\[ds-3, ds]` на основе логической даты Airflow

### \- \*\*Идемпотентность\*\* через `MERGE`, `ON CONFLICT DO UPDATE`, `DELETE + INSERT`

### \- \*\*Слоистая архитектура DWH\*\*: STG (сырые данные) → DDS (нормализованные факты и измерения) → CDM (агрегаты для BI)

### \- \*\*Параллельное выполнение\*\* независимых задач в DAG

### \- \*\*Три среза витрины\*\* (day / week / month) через `UNION ALL` из одного CTE

### 

### \## 🏗️ Архитектура

### 

### ```mermaid

### graph TD

### &#x20;   A\[GitHub: city.csv] -->|PythonOperator + pandas| B\[stg.city\_csv]

### &#x20;   C\[Open-Meteo: Weather API] -->|PythonOperator + requests| D\[stg.external\_weather\_api]

### &#x20;   E\[Open-Meteo: Sun/UV API] -->|PythonOperator + requests| F\[stg.external\_sun\_api]

### 

### &#x20;   B -->|MERGE| G\[dds.dim\_location]

### &#x20;   B -->|flatten JSON| D

### &#x20;   B -->|flatten JSON| F

### 

### &#x20;   G --> H\[dds.fct\_weather\_hourly]

### &#x20;   D --> H

### &#x20;   G --> I\[dds.fct\_sun\_metric]

### &#x20;   F --> I

### 

### &#x20;   H --> J\[cdm.agg\_weather]

### &#x20;   I --> J

### 

### &#x20;   style A fill:#f9f,stroke:#333

### &#x20;   style C fill:#f9f,stroke:#333

### &#x20;   style E fill:#f9f,stroke:#333

### &#x20;   style B fill:#bbf,stroke:#333

### &#x20;   style D fill:#bbf,stroke:#333

### &#x20;   style F fill:#bbf,stroke:#333

### &#x20;   style G fill:#bfb,stroke:#333

### &#x20;   style H fill:#bfb,stroke:#333

### &#x20;   style I fill:#bfb,stroke:#333

### &#x20;   style J fill:#fbb,stroke:#333

### ```

### 

### > 💡 Mermaid рендерится на GitHub автоматически. Цвета: розовый — источники, синий — STG, зелёный — DDS, красный — CDM.

### 

### \## 🔄 Граф DAG (8 тасок)

### 

### ```mermaid

### graph LR

### &#x20;   get\_city\[get\_city\_csv\_data] --> stg\_city\[stg\_city\_list]

### &#x20;   stg\_city --> stg\_weather\[stg\_external\_api]

### &#x20;   stg\_city --> stg\_sun\[stg\_sun\_api]

### &#x20;   stg\_city --> dim\_loc\[dds\_dim\_location]

### &#x20;   dim\_loc --> fct\_weather\[dds\_fct\_weather\_hourly]

### &#x20;   dim\_loc --> fct\_sun\[dds\_fct\_sun\_metric]

### &#x20;   stg\_weather --> fct\_weather

### &#x20;   stg\_sun --> fct\_sun

### &#x20;   fct\_weather --> cdm\[cdm\_agg\_weather]

### &#x20;   fct\_sun --> cdm

### ```

### 

### \## 🛠️ Технологический стек

### 

### | Компонент | Технология | Назначение |

### |-----------|-----------|------------|

### | Оркестрация | Apache Airflow 2.6.3 | Планирование и мониторинг DAG |

### | Язык | Python 3.8 | PythonOperator, pandas, requests |

### | Хранилище | PostgreSQL 18 | DWH (STG / DDS / CDM) |

### | Инфраструктура | Docker Desktop | Изоляция окружения |

### | IDE | DBeaver | Валидация данных |

### 

### \## 📂 Структура проекта

### 

### ```

### dags/

### ├── weather\_dag.py                    # Основной DAG, 8 тасок

### ├── lib/

### │   ├── \_\_init\_\_.py

### │   └── external\_api.py               # get\_point\_weather, get\_sun\_metric, flatten\_response

### └── sql/

### &#x20;   ├── stg\_city\_list\_w\_coords.sql

### &#x20;   ├── dds\_dim\_location.sql          # MERGE

### &#x20;   ├── dds\_fct\_weather\_hourly.sql    # DELETE + INSERT

### &#x20;   ├── dds\_fct\_sun\_metric.sql

### &#x20;   └── cdm\_agg\_weather.sql           # Три среза через CTE + UNION ALL

### ```

### 

### \## 🗄️ Схема данных

### 

### | Слой | Таблица | Строк | Стратегия загрузки |

### |------|---------|-------|-------------------|

### | STG | `city\_csv` | 1117 | `ON CONFLICT DO UPDATE` |

### | STG | `external\_weather\_api` | \~384 | `ON CONFLICT DO UPDATE` |

### | STG | `external\_sun\_api` | \~16 | `ON CONFLICT DO UPDATE` |

### | DDS | `dim\_location` | 1117 | `MERGE` |

### | DDS | `fct\_weather\_hourly` | \~384 | `DELETE + INSERT` |

### | DDS | `fct\_sun\_metric` | \~16 | `DELETE + INSERT` |

### | CDM | `agg\_weather` | day=16, week=4, month=4 | `DELETE + INSERT` |

### 

### \## 🔐 Ключевые технические решения

### 

### \- \*\*`{{ ds }}` vs `{{ macros.ds\_add(ds, -3) }}`\*\* — логическая дата и окно инкремента

### \- \*\*`MERGE`\*\* — идемпотентный upsert для измерений (PostgreSQL 15+)

### \- \*\*`ON CONFLICT DO UPDATE`\*\* — защита от дублей на STG

### \- \*\*`DELETE + INSERT`\*\* — полная перезапись фактов и витрин

### \- \*\*CTE + `UNION ALL`\*\* — три среза витрины без дублирования логики

### \- \*\*`catchup=False`\*\* — защита от лавинообразного запуска при старте

### 

### \## 🚀 Быстрый старт

### 

### ```bash

### \# 1. Клонировать репозиторий

### git clone https://github.com/your-username/weather-etl-airflow.git

### cd weather-etl-airflow

### 

### \# 2. Запустить Airflow в Docker

### docker compose up -d

### 

### \# 3. Открыть UI

### \# http://localhost:8080 — airflow / airflow

### 

### \# 4. Активировать DAG weather\_dag и запустить

### ```


# Blood Donors ETL: MinIO → PySpark → PostgreSQL → Superset

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.10.5-017CEE?logo=apache-airflow)
![Apache Spark](https://img.shields.io/badge/PySpark-3.5.4-E25A1C?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791?logo=postgresql)
![Superset](https://img.shields.io/badge/Superset-4.1.2-20A6C9)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

Учебный end-to-end ETL-пайплайн: сырой CSV лежит в S3-совместимом хранилище **MinIO**, **PySpark** читает его, приводит типы и проверяет качество, результат загружается в **PostgreSQL**, а **Apache Airflow** запускает всё по расписанию. Поверх таблицы можно строить дашборды в **Apache Superset**. Всё поднимается одной командой через Docker Compose.

## Архитектура

```mermaid
flowchart LR
    CSV[blood_donors.csv] -->|загрузка вручную| MinIO[(MinIO<br/>bucket raw-data)]
    Airflow[Airflow DAG<br/>blood_donors_etl<br/>каждый день в 15:00] -->|запускает| Spark[PySpark<br/>transform_blood_data.py]
    MinIO -->|s3a://| Spark
    Spark -->|JDBC, overwrite| PG[(PostgreSQL<br/>analytics.blood_analysis)]
    PG --> Superset[Apache Superset<br/>дашборды]
```

## Скриншоты

**MinIO:** бакет `raw-data` с исходным CSV

![MinIO](docs/minio.png)

**Airflow:** DAG `blood_donors_etl` успешно отработал

![Airflow](docs/airflow.png)

**Superset:** график по таблице `blood_analysis` (0 — не сдавал кровь в марте 2007, 1 — сдавал)

![Superset](docs/superset.png)

## Данные

Датасет **Blood Transfusion Service Center** (UCI Machine Learning Repository): 747 доноров, 5 колонок.

| Исходная колонка | Колонка в БД | Смысл |
|---|---|---|
| Recency (months) | `recency` | Месяцев с последней сдачи крови |
| Frequency (times) | `frequency` | Сколько раз человек сдавал кровь |
| Monetary (c.c. blood) | `monetary` | Суммарный объём сданной крови, мл |
| Time (months) | `time_months` | Месяцев с первой сдачи |
| whether he/she donated blood in March 2007 | `donated_march_2007` | Сдавал ли в марте 2007 (1 — да, 0 — нет) |

Источник: Yeh, I-Cheng, *Blood Transfusion Service Center* (2008), UCI Machine Learning Repository, лицензия CC BY 4.0.

## Что делает пайплайн

1. **Extract.** Spark читает `s3a://raw-data/blood_donors.csv` из MinIO.
2. **Transform.** Выбирает 5 колонок, приводит их к `int` и переименовывает в короткие имена.
3. **Validate.** Считает пропуски (NULL) и находит невозможные значения: отрицательные месяцы, нулевую частоту, флаг не 0/1. Такие строки выводятся в лог, но не удаляются.
4. **Load.** Записывает результат в таблицу `blood_analysis` базы `analytics` (режим `overwrite`, то есть при каждом запуске таблица пересоздаётся без дублей).

## Структура проекта

```
.
├── dags/
│   └── blood_donors_etl.py          # DAG Airflow: расписание и запуск скрипта
├── scripts/
│   ├── transform_blood_data.py      # основной ETL на PySpark
│   └── read_blood_data.py           # вспомогательный: быстрый просмотр данных в MinIO
├── docs/
│   └── *.png                        # скриншоты для README
├── data/
│   └── blood_donors.csv             # исходные данные
├── initdb/
│   └── 01-create-databases.sql      # создаёт базы analytics и superset
├── Dockerfile                       # образ Airflow + Java 17 + PySpark
├── Dockerfile.superset              # образ Superset + драйвер Postgres
├── docker-compose.yml               # все сервисы
├── superset_config.py               # настройки Superset
└── README.md
```

## Сервисы

| Сервис | Адрес | Логин / пароль |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO (консоль) | http://localhost:9001 | minioadmin / minioadmin123 |
| Superset | http://localhost:8088 | admin / admin |
| PostgreSQL | localhost:5433 | airflow / airflow |

> Все пароли учебные.

## Запуск

Нужны Docker и Docker Compose. Первая сборка занимает несколько минут.

**1. Поднять контейнеры**

```bash
docker compose up -d --build
```

**2. Положить данные в MinIO**

Откройте http://localhost:9001, войдите, создайте бакет **`raw-data`** и загрузите в него `data/blood_donors.csv`.

**3. Запустить DAG**

Откройте http://localhost:8081, найдите DAG `blood_donors_etl`, включите его (переключатель слева) и нажмите **Trigger DAG**. При первом запуске Spark скачивает JAR-библиотеки для S3 и Postgres, поэтому нужен интернет и это занимает дополнительное время. Логи задачи покажут схему, количество строк, пропуски и невалидные строки.

**4. Проверить результат**

Подключитесь к базе любым клиентом (DBeaver, pgAdmin, VS Code): `host=localhost`, `port=5433`, `database=analytics`, `user=airflow`, `password=airflow`.

```sql
SELECT * FROM blood_analysis LIMIT 10;
SELECT donated_march_2007, COUNT(*), ROUND(AVG(frequency), 1) AS avg_frequency
FROM blood_analysis
GROUP BY donated_march_2007;
```

**5. (Опционально) Дашборд в Superset**

В Superset: *Settings → Database Connections → + Database → PostgreSQL*, строка подключения:

```
postgresql://airflow:airflow@postgres:5432/analytics
```

Затем *Datasets → + Dataset* → таблица `blood_analysis`, и стройте графики.

## Остановка

```bash
docker compose down        # остановить, данные сохранятся
docker compose down -v     # остановить и удалить все данные (тома)
```
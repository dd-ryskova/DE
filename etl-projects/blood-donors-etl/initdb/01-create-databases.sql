-- Выполняется автоматически при ПЕРВОМ запуске Postgres (пустой том).
-- База "airflow" создаётся сама через POSTGRES_DB.
CREATE DATABASE analytics;  -- сюда Spark пишет результат
CREATE DATABASE superset;   -- служебная база Superset

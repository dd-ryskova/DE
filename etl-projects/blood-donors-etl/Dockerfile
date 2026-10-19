FROM apache/airflow:2.10.5-python3.11

USER root

RUN rm -f /etc/apt/sources.list.d/mariadb.list \
          /etc/apt/sources.list.d/pgdg.list \
          /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt \
    "apache-airflow==2.10.5" \
    apache-airflow-providers-amazon \
    pandas \
    boto3 \
    psycopg2-binary \
    pyspark==3.5.4
import os
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    spark = None

    try:
        # 1. Создаём Spark-сессию
        spark = (
            SparkSession.builder
            .appName("BloodDonorsTransformation")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.5"
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # 2. Настраиваем доступ к MinIO
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", os.environ["S3_ENDPOINT_URL"])
        hadoop_conf.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        hadoop_conf.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

        # 3. Читаем CSV за текущую дату
        load_date = date.today().isoformat()
        source_path = f"s3a://raw-data/blood_donors_{load_date}.csv"

        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")
            .csv(source_path)
        )

        print(f"\n=== SOURCE DATA: {source_path} ===")
        print(f"Rows: {raw_df.count()}")

        # 4. Переименовываем столбцы и добавляем дату загрузки
        df = raw_df.select(
            col("`Recency (months)`").cast("int").alias("recency"),
            col("`Frequency (times)`").cast("int").alias("frequency"),
            col("`Monetary (c.c. blood)`").cast("int").alias("monetary"),
            col("`Time (months)`").cast("int").alias("time_months"),
            col(
                "`whether he/she donated blood in March 2007`"
            ).cast("int").alias("donated_march_2007")
        ).withColumn("load_date", lit(load_date).cast("date"))

        # 5. Разделяем валидные и невалидные записи
        invalid_condition = (
            col("recency").isNull()
            | col("frequency").isNull()
            | col("monetary").isNull()
            | col("time_months").isNull()
            | col("donated_march_2007").isNull()
            | (col("recency") < 0)
            | (col("frequency") <= 0)
            | (col("monetary") < 0)
            | (col("time_months") < 0)
            | (~col("donated_march_2007").isin(0, 1))
        )

        valid_df = df.filter(~invalid_condition)
        invalid_df = df.filter(invalid_condition)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        print(f"\nValid rows: {valid_count}")
        print(f"Invalid rows: {invalid_count}")

        # 6. Настройки PostgreSQL
        postgres_url = "jdbc:postgresql://postgres:5432/analytics"

        connection_properties = {
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver"
        }

        batch_size = os.getenv("POSTGRES_BATCH_SIZE", "1000")
        partitions = int(os.getenv("POSTGRES_WRITE_PARTITIONS", "4"))

        def write_to_postgres(dataframe, table_name):
            (
                dataframe.repartition(partitions)
                .write
                .format("jdbc")
                .option("url", postgres_url)
                .option("dbtable", table_name)
                .option("batchsize", batch_size)
                .options(**connection_properties)
                .mode("append")
                .save()
            )
        
        # 7. Подготавливаем таблицы и очищаем данные за дату
        import psycopg2

        connection = None

        try:
            connection = psycopg2.connect(
                host="postgres",
                port=5432,
                dbname="analytics",
                user=connection_properties["user"],
                password=connection_properties["password"]
            )

            with connection:
                with connection.cursor() as cursor:
                    # Таблица для корректных записей
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS blood_analysis (
                            recency INTEGER,
                            frequency INTEGER,
                            monetary INTEGER,
                            time_months INTEGER,
                            donated_march_2007 INTEGER,
                            load_date DATE NOT NULL
                        )
                    """)

                    # Таблица для записей, не прошедших проверку
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS blood_analysis_rejected (
                            recency INTEGER,
                            frequency INTEGER,
                            monetary INTEGER,
                            time_months INTEGER,
                            donated_march_2007 INTEGER,
                            load_date DATE NOT NULL
                        )
                    """)

                    # Удаляем данные этой даты перед повторной загрузкой
                    cursor.execute(
                        "DELETE FROM blood_analysis WHERE load_date = %s",
                        (load_date,)
                    )

                    cursor.execute(
                        """
                        DELETE FROM blood_analysis_rejected
                        WHERE load_date = %s
                        """,
                        (load_date,)
                    )

        finally:
            if connection is not None:
                connection.close()

        # 8. Записываем валидные и невалидные записи
        if valid_count > 0:
            write_to_postgres(valid_df, "blood_analysis")

        if invalid_count > 0:
            write_to_postgres(invalid_df, "blood_analysis_rejected")

        print("\n=== ETL SUMMARY ===")
        print(f"Source rows: {valid_count + invalid_count}")
        print(f"Valid rows: {valid_count}")
        print(f"Invalid rows: {invalid_count}")
        print(f"Load date: {load_date}")
        print("ETL completed successfully")

    except Exception:
        logger.exception("ETL failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
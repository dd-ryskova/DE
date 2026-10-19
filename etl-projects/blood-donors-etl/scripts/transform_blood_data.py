import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
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

    # 3. Читаем исходный CSV
    source_path = "s3a://raw-data/blood_donors.csv"

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(source_path)
    )

    print("\n=== SOURCE DATA ===")
    print(f"Rows: {raw_df.count()}")

    # 4. Переименовываем столбцы
    df = raw_df.select(
        col("`Recency (months)`").cast("int").alias("recency"),
        col("`Frequency (times)`").cast("int").alias("frequency"),
        col("`Monetary (c.c. blood)`").cast("int").alias("monetary"),
        col("`Time (months)`").cast("int").alias("time_months"),
        col(
            "`whether he/she donated blood in March 2007`"
        ).cast("int").alias("donated_march_2007")
    )

    print("\n=== TRANSFORMED SCHEMA ===")
    df.printSchema()

    # 5. Проверяем количество пропусков
    print("\n=== NULL COUNTS ===")
    from pyspark.sql.functions import sum as spark_sum

    df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    # 6. Проверяем бизнес-правила
    invalid_df = df.filter(
        (col("recency") < 0)
        | (col("frequency") <= 0)
        | (col("monetary") < 0)
        | (col("time_months") < 0)
        | (~col("donated_march_2007").isin(0, 1))
    )

    print("\n=== INVALID ROWS ===")
    print(f"Invalid rows: {invalid_df.count()}")
    invalid_df.show(10, truncate=False)

    # 7. Показываем результат
    print("\n=== TRANSFORMED DATA ===")
    df.show(10, truncate=False)

    print("\n=== SUMMARY ===")
    print(f"Source rows: {raw_df.count()}")
    print(f"Transformed rows: {df.count()}")


    # 8. Загружаем данные в PostgreSQL
    postgres_url = "jdbc:postgresql://postgres:5432/analytics"

    connection_properties = {
        "user": "airflow",
        "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
        "driver": "org.postgresql.Driver"
    }

    print("\n=== WRITING TO POSTGRESQL ===")

    (
        df.write
        .format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "blood_analysis")
        .options(**connection_properties)
        .mode("overwrite")
        .save()
    )

    print("Data successfully written to analytics.blood_analysis")


    spark.stop()


if __name__ == "__main__":
    main()

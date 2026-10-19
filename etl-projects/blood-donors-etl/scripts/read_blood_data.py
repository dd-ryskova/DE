import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


def main():
    spark = (
        SparkSession.builder
        .appName("BloodDonorsReadFromMinIO")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )

    endpoint = os.environ["S3_ENDPOINT_URL"]
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    path = "s3a://raw-data/blood_donors.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(path)
    )

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== FIRST 5 ROWS ===")
    df.show(5, truncate=False)

    print("\n=== ROW COUNT ===")
    print(df.count())

    print("\n=== NULL COUNTS ===")
    null_counts = df.select([
        spark_sum(col(f"`{c}`").isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    null_counts.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

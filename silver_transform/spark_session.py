from pyspark.sql import SparkSession
import os


def get_spark(app_name: str = "health-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                    "software.amazon.awssdk:bundle:2.20.160",
                ]
            ),
        )
        # ADD THESE TWO LINES
        .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1")
        .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config(
            "spark.sql.catalog.iceberg.uri",
            os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
        )
        .config(
            "spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.catalog.iceberg.s3.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        )
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        )
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = get_spark("test-connection")
    df = spark.sql("SHOW NAMESPACES IN iceberg")
    df.show()
    spark.stop()
    print("Spark + Iceberg connection OK")

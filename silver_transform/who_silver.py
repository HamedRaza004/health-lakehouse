from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime, timezone


def transform_who_to_silver():
    spark = get_spark("who-bronze-to-silver")
    print("Reading bronze.who_gho_raw...")

    bronze = spark.sql("SELECT * FROM iceberg.bronze.who_gho_raw")
    print(f"Bronze row count: {bronze.count()}")

    silver = (
        bronze
        # --- type coercion ---
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("low", F.col("low").cast(DoubleType()))
        .withColumn("high", F.col("high").cast(DoubleType()))
        # --- country code normalization: uppercase, trim whitespace ---
        .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
        # --- drop rows with no country or no year (unusable for analysis) ---
        .filter(F.col("country_code").isNotNull())
        .filter(F.col("year").isNotNull())
        .filter(F.col("indicator_code").isNotNull())
        # --- clip obviously bad values ---
        .withColumn("value", F.when(F.col("value") < 0, None).otherwise(F.col("value")))
        # --- standardize sex field ---
        .withColumn(
            "sex",
            F.when(F.col("sex") == "BTSX", "both")
            .when(F.col("sex") == "MLE", "male")
            .when(F.col("sex") == "FMLE", "female")
            .otherwise(F.col("sex")),
        )
        # --- deduplication: keep latest ingested record per key ---
        .withColumn(
            "_row_num",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("indicator_code", "country_code", "year", "sex")
                .orderBy(F.col("_ingested_at").desc())
            ),
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        # --- add silver metadata ---
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )

    print(f"Silver row count after cleaning: {silver.count()}")

    silver.writeTo("iceberg.silver.who_gho_silver").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    print("Written to iceberg.silver.who_gho_silver")
    spark.stop()


if __name__ == "__main__":
    transform_who_to_silver()

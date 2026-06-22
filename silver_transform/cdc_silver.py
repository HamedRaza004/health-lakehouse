from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timezone


def transform_cdc_to_silver():
    spark = get_spark("cdc-bronze-to-silver")
    print("Reading bronze.cdc_nndss_raw...")

    bronze = spark.sql("SELECT * FROM iceberg.bronze.cdc_nndss_raw")
    print(f"Bronze row count: {bronze.count()}")

    window_dedup = Window.partitionBy(
        "condition", "reporting_area", "year", "week"
    ).orderBy(F.col("_ingested_at").desc())

    window_rolling = (
        Window.partitionBy("condition", "reporting_area")
        .orderBy("year", "week")
        .rowsBetween(-4, -1)
    )

    silver = (
        bronze.withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("week", F.col("week").cast(IntegerType()))
        .withColumn("current_week", F.col("current_week").cast(DoubleType()))
        .withColumn(
            "cumulative_ytd_current_year",
            F.col("cumulative_ytd_current_year").cast(DoubleType()),
        )
        .withColumn(
            "cumulative_ytd_previous_year",
            F.col("cumulative_ytd_previous_year").cast(DoubleType()),
        )
        .withColumn("reporting_area", F.upper(F.trim(F.col("reporting_area"))))
        .withColumn("condition", F.upper(F.trim(F.col("condition"))))
        .filter(F.col("condition").isNotNull())
        .filter(F.col("reporting_area").isNotNull())
        .filter(F.col("year").isNotNull())
        .withColumn(
            "current_week",
            F.when(F.col("current_week") < 0, None).otherwise(F.col("current_week")),
        )
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .withColumn("rolling_avg_cases", F.avg("current_week").over(window_rolling))
        .withColumn(
            "is_outbreak",
            F.when(
                F.col("current_week") > 3 * F.col("rolling_avg_cases"), True
            ).otherwise(False),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )

    print(f"Silver row count after cleaning: {silver.count()}")

    silver.writeTo("iceberg.silver.cdc_nndss_silver").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    print("Written to iceberg.silver.cdc_nndss_silver")
    spark.stop()


if __name__ == "__main__":
    transform_cdc_to_silver()

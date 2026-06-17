import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from pyspark.sql.window import Window
from datetime import datetime, timezone

FRED_TABLE_MAP = {
    "iceberg.bronze.fred_unrate_raw": "iceberg.silver.fred_unrate_silver",
    "iceberg.bronze.fred_cpiaucsl_raw": "iceberg.silver.fred_cpiaucsl_silver",
    "iceberg.bronze.fred_dgs10_raw": "iceberg.silver.fred_dgs10_silver",
}


def transform_single_fred(spark, bronze_table: str, silver_table: str):
    print(f"  Reading {bronze_table}...")
    bronze = spark.sql(f"SELECT * FROM {bronze_table}")
    count_in = bronze.count()
    print(f"  Bronze row count: {count_in}")

    if count_in == 0:
        print(f"  SKIP — no rows in {bronze_table}")
        return

    # schema inside record_json
    json_schema = StructType(
        [
            StructField("series_id", StringType(), True),
            StructField("observation_date", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    # parse record_json → flat columns
    parsed = bronze.withColumn(
        "parsed", F.from_json(F.col("record_json"), json_schema)
    ).select(
        F.col("parsed.series_id").alias("series_id"),
        F.col("parsed.observation_date").alias("date_str"),
        F.col("parsed.value").alias("value_str"),
        F.col("_ingested_at"),
        F.col("_source_url"),
        F.col("_ingestion_date"),
    )

    window_dedup = Window.partitionBy("series_id", "date").orderBy(
        F.col("_ingested_at").desc()
    )

    window_yoy = Window.partitionBy("series_id").orderBy("date").rowsBetween(-365, -1)

    silver = (
        parsed.withColumn("date", F.to_date(F.col("date_str"), "yyyy-MM-dd"))
        .withColumn("value", F.col("value_str").cast(DoubleType()))
        .drop("date_str", "value_str")
        .filter(F.col("series_id").isNotNull())
        .filter(F.col("date").isNotNull())
        .filter(F.col("value").isNotNull())
        .withColumn(
            "_row_num",
            F.row_number().over(
                Window.partitionBy("series_id", "date").orderBy(
                    F.col("_ingested_at").desc()
                )
            ),
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .withColumn(
            "prev_year_value",
            F.avg("value").over(
                Window.partitionBy("series_id").orderBy("date").rowsBetween(-365, -1)
            ),
        )
        .withColumn(
            "yoy_pct_change",
            F.when(
                F.col("prev_year_value").isNotNull() & (F.col("prev_year_value") != 0),
                F.round(
                    (
                        (F.col("value") - F.col("prev_year_value"))
                        / F.col("prev_year_value")
                    )
                    * 100,
                    2,
                ),
            ).otherwise(None),
        )
        .drop("prev_year_value")
        .withColumn(
            "_silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat())
        )
        .withColumn("_silver_version", F.lit(1))
    )

    count_out = silver.count()
    silver.writeTo(silver_table).tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()
    print(f"  Written {count_out} rows → {silver_table}")


def transform_fred_to_silver():
    spark = get_spark("fred-bronze-to-silver")
    for bronze_table, silver_table in FRED_TABLE_MAP.items():
        transform_single_fred(spark, bronze_table, silver_table)
    spark.stop()
    print("FRED silver transforms done.")


if __name__ == "__main__":
    transform_fred_to_silver()

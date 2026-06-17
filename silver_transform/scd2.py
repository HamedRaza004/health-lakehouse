import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_scd2_table(
    source_table: str, target_table: str, key_cols: list, value_cols: list
):
    spark = get_spark("scd2-builder")
    df = spark.sql(f"SELECT * FROM {source_table}")
    print(f"Source rows: {df.count()}")

    window = Window.partitionBy(*key_cols).orderBy("year")

    scd = (
        df.withColumn("valid_from", F.col("year").cast("string"))
        .withColumn("valid_to", F.lead("year", 1).over(window).cast("string"))
        .withColumn(
            "valid_to",
            F.when(F.col("valid_to").isNull(), F.lit("9999")).otherwise(
                F.col("valid_to")
            ),
        )
        .withColumn("is_current", F.col("valid_to") == F.lit("9999"))
        .withColumn(
            "_scd_hash",
            F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in value_cols])),
        )
    )

    scd.writeTo(target_table).tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    total = scd.count()
    current = scd.filter(F.col("is_current")).count()
    print(f"SCD2 written: {total:,} total, {current:,} current records")
    spark.stop()


if __name__ == "__main__":
    build_scd2_table(
        source_table="iceberg.silver.who_gho_silver",
        target_table="iceberg.silver.who_gho_scd2",
        key_cols=["indicator_code", "country_code", "sex"],
        value_cols=["value", "low", "high"],
    )

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from datetime import datetime, timezone


def transform_fda_to_silver():
    spark = get_spark("fda-bronze-to-silver")
    print("Reading bronze.openfda_drug_event_raw...")

    bronze = spark.sql("SELECT * FROM iceberg.bronze.openfda_drug_event_raw")

    print(f"Bronze row count: {bronze.count()}")

    extracted = (
        bronze.withColumn(
            "report_id", F.get_json_object("record_json", "$.safetyreportid")
        )
        .withColumn(
            "drug_name",
            F.upper(
                F.trim(
                    F.get_json_object(
                        "record_json", "$.patient.drug[0].medicinalproduct"
                    )
                )
            ),
        )
        .withColumn(
            "serious", F.get_json_object("record_json", "$.serious").cast(IntegerType())
        )
        .withColumn(
            "receive_date",
            F.to_date(F.get_json_object("record_json", "$.receivedate"), "yyyyMMdd"),
        )
        .withColumn(
            "patient_age",
            F.get_json_object("record_json", "$.patient.patientonsetage").cast(
                IntegerType()
            ),
        )
        .withColumn(
            "patient_sex", F.get_json_object("record_json", "$.patient.patientsex")
        )
    )

    window_dedup = Window.partitionBy("report_id", "drug_name").orderBy(
        F.col("_ingested_at").desc()
    )

    silver = (
        extracted.filter(F.col("report_id").isNotNull())
        .filter(F.col("drug_name").isNotNull())
        .withColumn(
            "patient_age",
            F.when(
                (F.col("patient_age") < 0) | (F.col("patient_age") > 120), None
            ).otherwise(F.col("patient_age")),
        )
        .withColumn(
            "patient_sex",
            F.when(F.col("patient_sex") == "1", "male")
            .when(F.col("patient_sex") == "2", "female")
            .otherwise("unknown"),
        )
        .withColumn("is_serious", F.when(F.col("serious") == 1, True).otherwise(False))
        .withColumn(
            "serious_label",
            F.when(F.col("serious") == 1, "serious").otherwise("non-serious"),
        )
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )

    print(f"Silver row count after cleaning: {silver.count()}")

    silver.writeTo("iceberg.silver.openfda_drug_event_silver").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    print("Written to iceberg.silver.openfda_drug_event_silver")

    spark.stop()


if __name__ == "__main__":
    transform_fda_to_silver()

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timezone

CONTINENT_NAMES = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
    "High income",
    "Low income",
    "Upper middle income",
    "Lower middle income",
    "European Union",
]


def transform_owid_to_silver():
    spark = get_spark("owid-bronze-to-silver")

    # ==========================================================
    # COVID DATASET
    # ==========================================================

    print("Reading bronze.owid_covid_raw...")

    covid_bronze = spark.sql("SELECT * FROM iceberg.bronze.owid_covid_raw")

    print(f"COVID bronze row count: {covid_bronze.count()}")

    covid = (
        covid_bronze.withColumn(
            "iso_code", get_json_object("record_json", "$.iso_code")
        )
        .withColumn("location", get_json_object("record_json", "$.location"))
        .withColumn("continent", get_json_object("record_json", "$.continent"))
        .withColumn("date", get_json_object("record_json", "$.date"))
        .withColumn("new_cases", get_json_object("record_json", "$.new_cases"))
        .withColumn("total_cases", get_json_object("record_json", "$.total_cases"))
        .withColumn("new_deaths", get_json_object("record_json", "$.new_deaths"))
        .withColumn("total_deaths", get_json_object("record_json", "$.total_deaths"))
        .withColumn(
            "new_vaccinations", get_json_object("record_json", "$.new_vaccinations")
        )
        .withColumn(
            "total_vaccinations", get_json_object("record_json", "$.total_vaccinations")
        )
        .withColumn(
            "people_vaccinated", get_json_object("record_json", "$.people_vaccinated")
        )
        .withColumn(
            "people_fully_vaccinated",
            get_json_object("record_json", "$.people_fully_vaccinated"),
        )
        .withColumn(
            "reproduction_rate", get_json_object("record_json", "$.reproduction_rate")
        )
        .withColumn("icu_patients", get_json_object("record_json", "$.icu_patients"))
        .withColumn("hosp_patients", get_json_object("record_json", "$.hosp_patients"))
    )

    numeric_cols = [
        "new_cases",
        "total_cases",
        "new_deaths",
        "total_deaths",
        "new_vaccinations",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "reproduction_rate",
        "icu_patients",
        "hosp_patients",
    ]

    for col_name in numeric_cols:
        covid = covid.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    window_dedup = Window.partitionBy("iso_code", "date").orderBy(
        F.col("_ingested_at").desc()
    )

    window_rolling = Window.partitionBy("iso_code").orderBy("date").rowsBetween(-6, 0)

    covid_silver = (
        covid.withColumn("date", F.to_date("date"))
        .filter(F.col("iso_code").isNotNull())
        .filter(F.col("date").isNotNull())
        .filter(~F.col("location").isin(CONTINENT_NAMES))
        .withColumn(
            "new_cases",
            F.when(F.col("new_cases") < 0, None).otherwise(F.col("new_cases")),
        )
        .withColumn(
            "new_deaths",
            F.when(F.col("new_deaths") < 0, None).otherwise(F.col("new_deaths")),
        )
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .withColumn("rolling_7d_avg_cases", F.avg("new_cases").over(window_rolling))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )

    print(f"COVID silver row count: {covid_silver.count()}")

    covid_silver.writeTo("iceberg.silver.owid_covid_silver").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    print("Written to iceberg.silver.owid_covid_silver")

    # ==========================================================
    # VACCINATION DATASET
    # ==========================================================

    print("Reading bronze.owid_vaccination_raw...")

    vax_bronze = spark.sql("SELECT * FROM iceberg.bronze.owid_vaccination_raw")

    vax = (
        vax_bronze.withColumn("iso_code", get_json_object("record_json", "$.iso_code"))
        .withColumn("location", get_json_object("record_json", "$.location"))
        .withColumn("date", get_json_object("record_json", "$.date"))
        .withColumn(
            "total_vaccinations", get_json_object("record_json", "$.total_vaccinations")
        )
        .withColumn(
            "people_vaccinated", get_json_object("record_json", "$.people_vaccinated")
        )
        .withColumn(
            "people_fully_vaccinated",
            get_json_object("record_json", "$.people_fully_vaccinated"),
        )
        .withColumn(
            "daily_vaccinations", get_json_object("record_json", "$.daily_vaccinations")
        )
        .withColumn(
            "total_vaccinations_per_hundred",
            get_json_object("record_json", "$.total_vaccinations_per_hundred"),
        )
        .withColumn(
            "people_vaccinated_per_hundred",
            get_json_object("record_json", "$.people_vaccinated_per_hundred"),
        )
        .withColumn(
            "people_fully_vaccinated_per_hundred",
            get_json_object("record_json", "$.people_fully_vaccinated_per_hundred"),
        )
    )

    vax_numeric = [
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "daily_vaccinations",
        "total_vaccinations_per_hundred",
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
    ]

    for col_name in vax_numeric:
        vax = vax.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    window_dedup_vax = Window.partitionBy("iso_code", "date").orderBy(
        F.col("_ingested_at").desc()
    )

    vax_silver = (
        vax.withColumn("date", F.to_date("date"))
        .filter(F.col("iso_code").isNotNull())
        .filter(F.col("date").isNotNull())
        .withColumn("_row_num", F.row_number().over(window_dedup_vax))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )

    print(f"Vaccination silver row count: {vax_silver.count()}")

    vax_silver.writeTo("iceberg.silver.owid_vaccination_silver").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    print("Written to iceberg.silver.owid_vaccination_silver")

    spark.stop()


if __name__ == "__main__":
    transform_owid_to_silver()

import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType
from dotenv import load_dotenv

load_dotenv()

CATALOG_CONFIG = {
    "type": "rest",
    "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
    "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "s3.path-style-access": "true",
}

CDC_URL = "https://data.cdc.gov/resource/x9gk-5huc.json"

BRONZE_SCHEMA = Schema(
    NestedField(1, "reporting_area", StringType(), required=False),
    NestedField(2, "year", IntegerType(), required=False),
    NestedField(3, "week", IntegerType(), required=False),
    NestedField(4, "condition", StringType(), required=False),
    NestedField(5, "current_week", DoubleType(), required=False),
    NestedField(6, "current_week_flag", StringType(), required=False),
    NestedField(7, "previous_52_week_max", DoubleType(), required=False),
    NestedField(8, "previous_52_week_max_flag", StringType(), required=False),
    NestedField(9, "cumulative_ytd_current_year", DoubleType(), required=False),
    NestedField(10, "cumulative_ytd_current_year_flag", StringType(), required=False),
    NestedField(11, "cumulative_ytd_previous_year", DoubleType(), required=False),
    NestedField(12, "cumulative_ytd_previous_year_flag", StringType(), required=False),
    NestedField(13, "location1", StringType(), required=False),
    NestedField(14, "location2", StringType(), required=False),
    NestedField(15, "longitude", DoubleType(), required=False),
    NestedField(16, "latitude", DoubleType(), required=False),
    NestedField(17, "sort_order", StringType(), required=False),
    NestedField(18, "_ingested_at", StringType(), required=True),
    NestedField(19, "_source_url", StringType(), required=True),
    NestedField(20, "_ingestion_date", StringType(), required=True),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("reporting_area", pa.string(), nullable=True),
        pa.field("year", pa.int32(), nullable=True),
        pa.field("week", pa.int32(), nullable=True),
        pa.field("condition", pa.string(), nullable=True),
        pa.field("current_week", pa.float64(), nullable=True),
        pa.field("current_week_flag", pa.string(), nullable=True),
        pa.field("previous_52_week_max", pa.float64(), nullable=True),
        pa.field("previous_52_week_max_flag", pa.string(), nullable=True),
        pa.field("cumulative_ytd_current_year", pa.float64(), nullable=True),
        pa.field("cumulative_ytd_current_year_flag", pa.string(), nullable=True),
        pa.field("cumulative_ytd_previous_year", pa.float64(), nullable=True),
        pa.field("cumulative_ytd_previous_year_flag", pa.string(), nullable=True),
        pa.field("location1", pa.string(), nullable=True),
        pa.field("location2", pa.string(), nullable=True),
        pa.field("longitude", pa.float64(), nullable=True),
        pa.field("latitude", pa.float64(), nullable=True),
        pa.field("sort_order", pa.string(), nullable=True),
        pa.field("_ingested_at", pa.string(), nullable=False),
        pa.field("_source_url", pa.string(), nullable=False),
        pa.field("_ingestion_date", pa.string(), nullable=False),
    ]
)


def parse_int(value):
    parsed = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(parsed) else int(parsed)


def parse_float(value):
    parsed = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(parsed) else float(parsed)


def parse_point(point: dict | None) -> tuple[float | None, float | None]:
    if not isinstance(point, dict):
        return None, None

    coordinates = point.get("coordinates") or []
    if len(coordinates) < 2:
        return None, None

    return parse_float(coordinates[0]), parse_float(coordinates[1])


def fetch_cdc(limit=5000) -> list[dict]:
    records, offset = [], 0
    while True:
        resp = requests.get(
            CDC_URL, params={"$limit": limit, "$offset": offset}, timeout=30
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        records.extend(batch)
        offset += limit
        print(f"  Fetched {len(records)} records so far...")
    return records


def parse_cdc(raw: list[dict]) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for r in raw:
        longitude, latitude = parse_point(r.get("geocode"))
        rows.append(
            {
                "reporting_area": r.get("states"),
                "year": parse_int(r.get("year")),
                "week": parse_int(r.get("week")),
                "condition": r.get("label"),
                "current_week": parse_float(r.get("m1")),
                "current_week_flag": r.get("m1_flag"),
                "previous_52_week_max": parse_float(r.get("m2")),
                "previous_52_week_max_flag": r.get("m2_flag"),
                "cumulative_ytd_current_year": parse_float(r.get("m3")),
                "cumulative_ytd_current_year_flag": r.get("m3_flag"),
                "cumulative_ytd_previous_year": parse_float(r.get("m4")),
                "cumulative_ytd_previous_year_flag": r.get("m4_flag"),
                "location1": r.get("location1"),
                "location2": r.get("location2"),
                "longitude": longitude,
                "latitude": latitude,
                "sort_order": r.get("sort_order"),
                "_ingested_at": now,
                "_source_url": CDC_URL,
                "_ingestion_date": today,
            }
        )
    return pd.DataFrame(rows, columns=ARROW_SCHEMA.names)


def run():
    catalog = load_catalog("default", **CATALOG_CONFIG)
    print("Fetching CDC NNDSS data...")
    raw = fetch_cdc()
    df = parse_cdc(raw)
    print(f"Total records: {len(df)}")
    arrow_table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)
    table_name = "bronze.cdc_nndss_raw"
    if catalog.table_exists(table_name):
        catalog.load_table(table_name).append(arrow_table)
    else:
        tbl = catalog.create_table(
            table_name,
            schema=BRONZE_SCHEMA,
            location="s3://lakehouse/bronze/cdc_nndss_raw",
        )
        tbl.append(arrow_table)
    print("Done.")


if __name__ == "__main__":
    run()

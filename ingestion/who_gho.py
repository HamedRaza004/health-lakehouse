import requests
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from datetime import datetime, timezone
import os
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

WHO_BASE = "https://ghoapi.azureedge.net/api"
INDICATORS = ["WHS4_100", "MDG_0000000001", "WHOSIS_000001"]

BRONZE_SCHEMA = Schema(
    NestedField(1, "indicator_code", StringType(), required=True),
    NestedField(2, "indicator_name", StringType(), required=False),
    NestedField(3, "country_code", StringType(), required=False),
    NestedField(4, "country_name", StringType(), required=False),
    NestedField(5, "year", IntegerType(), required=False),
    NestedField(6, "value", DoubleType(), required=False),
    NestedField(7, "low", DoubleType(), required=False),
    NestedField(8, "high", DoubleType(), required=False),
    NestedField(9, "sex", StringType(), required=False),
    NestedField(10, "_ingested_at", StringType(), required=True),
    NestedField(11, "_source_url", StringType(), required=True),
    NestedField(12, "_ingestion_date", StringType(), required=True),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("indicator_code", pa.string(), nullable=False),
        pa.field("indicator_name", pa.string(), nullable=True),
        pa.field("country_code", pa.string(), nullable=True),
        pa.field("country_name", pa.string(), nullable=True),
        pa.field("year", pa.int32(), nullable=True),
        pa.field("value", pa.float64(), nullable=True),
        pa.field("low", pa.float64(), nullable=True),
        pa.field("high", pa.float64(), nullable=True),
        pa.field("sex", pa.string(), nullable=True),
        pa.field("_ingested_at", pa.string(), nullable=False),
        pa.field("_source_url", pa.string(), nullable=False),
        pa.field("_ingestion_date", pa.string(), nullable=False),
    ]
)


def fetch_indicator(indicator_code: str) -> list[dict]:
    url = f"{WHO_BASE}/{indicator_code}"
    print(f"  Fetching {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("value", [])


def parse_records(
    raw: list[dict], indicator_code: str, source_url: str
) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for r in raw:
        rows.append(
            {
                "indicator_code": indicator_code,
                "indicator_name": r.get("IndicatorName"),
                "country_code": r.get("SpatialDim"),
                "country_name": r.get("SpatialDim"),
                "year": r.get("TimeDim"),
                "value": r.get("NumericValue"),
                "low": r.get("Low"),
                "high": r.get("High"),
                "sex": r.get("Dim1"),
                "_ingested_at": now,
                "_source_url": source_url,
                "_ingestion_date": today,
            }
        )
    return pd.DataFrame(rows)


def write_to_bronze(df: pd.DataFrame, catalog):
    table_name = "bronze.who_gho_raw"
    # Required string columns
    required_cols = ["indicator_code", "_ingested_at", "_source_url", "_ingestion_date"]

    for col in required_cols:
        df[col] = df[col].fillna("").astype(str)

    # Optional string columns
    string_cols = ["indicator_name", "country_code", "country_name", "sex"]

    for col in string_cols:
        df[col] = df[col].astype("string")

    # Integer column
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int32")

    # Float columns
    float_cols = ["value", "low", "high"]

    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    arrow_table = pa.Table.from_pandas(
        df,
        schema=ARROW_SCHEMA,
        preserve_index=False,
    )

    if catalog.table_exists(table_name):
        tbl = catalog.load_table(table_name)
        tbl.append(arrow_table)
        print(f"  Appended {len(df)} rows to {table_name}")
    else:
        tbl = catalog.create_table(
            table_name,
            schema=BRONZE_SCHEMA,
            location="s3://lakehouse/bronze/who_gho_raw",
        )
        tbl.append(arrow_table)
        print(f"  Created {table_name} and wrote {len(df)} rows")


def run():
    print("Connecting to Iceberg catalog...")
    catalog = load_catalog("default", **CATALOG_CONFIG)

    all_dfs = []
    for code in INDICATORS:
        url = f"{WHO_BASE}/{code}"
        print(f"\nProcessing indicator: {code}")
        raw = fetch_indicator(code)
        print(f"  Fetched {len(raw)} raw records")
        df = parse_records(raw, code, url)
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal records to write: {len(combined)}")
    write_to_bronze(combined, catalog)
    print("\nDone.")


if __name__ == "__main__":
    run()

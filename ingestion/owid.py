import json
import os
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import pyarrow as pa
import requests
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

load_dotenv()

CATALOG_CONFIG = {
    "type": "rest",
    "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
    "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "s3.path-style-access": "true",
}

OWID_SOURCES = {
    "covid": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
    "vaccination": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv",
}

BRONZE_SCHEMA = Schema(
    NestedField(1, "dataset", StringType(), required=True),
    NestedField(2, "source_row_number", IntegerType(), required=True),
    NestedField(3, "record_json", StringType(), required=True),
    NestedField(4, "_ingested_at", StringType(), required=True),
    NestedField(5, "_source_url", StringType(), required=True),
    NestedField(6, "_ingestion_date", StringType(), required=True),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int32(), nullable=False),
        pa.field("record_json", pa.string(), nullable=False),
        pa.field("_ingested_at", pa.string(), nullable=False),
        pa.field("_source_url", pa.string(), nullable=False),
        pa.field("_ingestion_date", pa.string(), nullable=False),
    ]
)

EXPECTED_COLUMNS = [field.name for field in BRONZE_SCHEMA.fields]


def normalize_column_name(column_name: str) -> str:
    return column_name.strip().lower().replace(" ", "_")


def fetch_owid(name: str, url: str) -> pd.DataFrame:
    print(f"  Fetching OWID {name} from {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    df.columns = [normalize_column_name(column) for column in df.columns]
    return df


def parse_owid(df: pd.DataFrame, name: str, url: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    clean_df = df.astype(object).where(pd.notna(df), None)

    rows = []
    for row_number, record in enumerate(clean_df.to_dict(orient="records"), start=1):
        rows.append(
            {
                "dataset": name,
                "source_row_number": row_number,
                "record_json": json.dumps(record, sort_keys=True, default=str),
                "_ingested_at": now,
                "_source_url": url,
                "_ingestion_date": today,
            }
        )

    return pd.DataFrame(rows, columns=ARROW_SCHEMA.names)


def write_to_bronze(df: pd.DataFrame, catalog, table_name: str, location: str):
    arrow_table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)

    if catalog.table_exists(table_name):
        tbl = catalog.load_table(table_name)
        current_columns = [field.name for field in tbl.schema().fields]
        if current_columns != EXPECTED_COLUMNS:
            raise RuntimeError(
                f"{table_name} already exists with an incompatible schema. "
                "Drop or rename the old table, then rerun this ingester."
            )
        tbl.append(arrow_table)
        print(f"  Appended {len(df)} rows to {table_name}")
    else:
        tbl = catalog.create_table(
            table_name,
            schema=BRONZE_SCHEMA,
            location=location,
        )
        tbl.append(arrow_table)
        print(f"  Created {table_name} and wrote {len(df)} rows")


def run():
    catalog = load_catalog("default", **CATALOG_CONFIG)
    for name, url in OWID_SOURCES.items():
        raw_df = fetch_owid(name, url)
        bronze_df = parse_owid(raw_df, name, url)
        print(f"  {name}: {len(bronze_df)} rows, {len(raw_df.columns)} source columns")
        write_to_bronze(
            bronze_df,
            catalog,
            table_name=f"bronze.owid_{name}_raw",
            location=f"s3://lakehouse/bronze/owid_{name}_raw",
        )
    print("Done.")


if __name__ == "__main__":
    run()

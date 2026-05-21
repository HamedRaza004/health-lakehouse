import json
import math
import os
from datetime import datetime, timezone

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

OPENFDA_SOURCES = {
    "drug_event": "https://api.fda.gov/drug/event.json",
}

DEFAULT_TARGET_RECORDS = 5000
DEFAULT_API_KEY_LIMIT = 1000
DEFAULT_ANONYMOUS_LIMIT = 100
MAX_OPENFDA_LIMIT = 1000
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": os.getenv("OPENFDA_USER_AGENT", "Mozilla/5.0"),
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


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc


def default_limit(api_key: str | None) -> int:
    return DEFAULT_API_KEY_LIMIT if api_key else DEFAULT_ANONYMOUS_LIMIT


def openfda_error(resp: requests.Response, limit: int) -> RuntimeError:
    body = resp.text[:500].replace("\n", " ")
    return RuntimeError(
        f"OpenFDA request failed with HTTP {resp.status_code} using limit={limit}. "
        "If this keeps happening, set OPENFDA_API_KEY in .env or lower "
        f"OPENFDA_LIMIT. Response body: {body}"
    )


def fetch_openfda(name: str, url: str) -> list[dict]:
    api_key = os.getenv("OPENFDA_API_KEY")
    limit = min(max(env_int("OPENFDA_LIMIT", default_limit(api_key)), 1), MAX_OPENFDA_LIMIT)
    target_records = max(env_int("OPENFDA_TARGET_RECORDS", DEFAULT_TARGET_RECORDS), 1)
    max_pages_configured = bool(os.getenv("OPENFDA_MAX_PAGES"))
    max_pages = max(
        env_int("OPENFDA_MAX_PAGES", math.ceil(target_records / limit)),
        1,
    )
    search = os.getenv(f"OPENFDA_{name.upper()}_SEARCH") or os.getenv("OPENFDA_SEARCH")

    records = []
    page = 0
    skip = 0
    while page < max_pages and len(records) < target_records:
        page += 1
        page_limit = min(limit, target_records - len(records))
        params = {"limit": page_limit, "skip": skip}
        if api_key:
            params["api_key"] = api_key
        if search:
            params["search"] = search

        print(f"  Fetching OpenFDA {name}: page {page}/{max_pages}")
        resp = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=60)
        if resp.status_code == 403 and not api_key and page_limit > DEFAULT_ANONYMOUS_LIMIT:
            print(
                "  OpenFDA rejected the larger anonymous page size; "
                f"retrying with limit={DEFAULT_ANONYMOUS_LIMIT}"
            )
            limit = DEFAULT_ANONYMOUS_LIMIT
            page_limit = min(limit, target_records - len(records))
            params["limit"] = page_limit
            if not max_pages_configured:
                remaining = target_records - len(records)
                max_pages = page - 1 + math.ceil(remaining / limit)
            resp = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=60)

        if resp.status_code == 404:
            break
        if resp.status_code == 403:
            raise openfda_error(resp, page_limit)
        resp.raise_for_status()

        batch = resp.json().get("results", [])
        if not batch:
            break

        records.extend(batch)
        skip += page_limit
        if len(batch) < page_limit:
            break

    return records


def parse_openfda(records: list[dict], name: str, url: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    rows = []
    for row_number, record in enumerate(records, start=1):
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
    for name, url in OPENFDA_SOURCES.items():
        records = fetch_openfda(name, url)
        df = parse_openfda(records, name, url)
        print(f"  {name}: {len(df)} records")
        write_to_bronze(
            df,
            catalog,
            table_name=f"bronze.openfda_{name}_raw",
            location=f"s3://lakehouse/bronze/openfda_{name}_raw",
        )
    print("Done.")


if __name__ == "__main__":
    run()

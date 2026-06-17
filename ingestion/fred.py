import requests, pandas as pd, pyarrow as pa, os
from pyiceberg.catalog import load_catalog
from datetime import datetime, timezone
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

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY")

# each series gets its own bronze table
SERIES_TABLE_MAP = {
    "UNRATE": "bronze.fred_unrate_raw",
    "CPIAUCSL": "bronze.fred_cpiaucsl_raw",
    "DGS10": "bronze.fred_dgs10_raw",
}


def fetch_series(series_id: str) -> list[dict]:
    resp = requests.get(
        FRED_BASE,
        params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("observations", [])


def run():
    catalog = load_catalog("default", **CATALOG_CONFIG)
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for sid, table_name in SERIES_TABLE_MAP.items():
        print(f"  Fetching FRED series: {sid}")
        obs = fetch_series(sid)
        print(f"  Got {len(obs)} observations for {sid}")

        rows = []
        for o in obs:
            rows.append(
                {
                    "series_id": sid,
                    "date": o.get("date"),
                    "value": float(o["value"])
                    if o.get("value") not in [".", None]
                    else None,
                    "_ingested_at": now,
                    "_source_url": FRED_BASE,
                    "_ingestion_date": today,
                }
            )

        df = pd.DataFrame(rows)
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        location = f"s3://lakehouse/bronze/{table_name.split('.')[1]}"

        if catalog.table_exists(table_name):
            catalog.load_table(table_name).append(arrow_table)
            print(f"  Appended {len(df)} rows → {table_name}")
        else:
            tbl = catalog.create_table(
                table_name,
                schema=None,
                location=location,
            )
            tbl.append(arrow_table)
            print(f"  Created and wrote {len(df)} rows → {table_name}")

    print("FRED ingestion done.")


if __name__ == "__main__":
    run()

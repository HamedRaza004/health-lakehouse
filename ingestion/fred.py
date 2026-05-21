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
SERIES_IDS = ["HLTHSCPCHCSA", "PPAACH", "UNRATE", "GDPC1", "POPTHM"]


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
    all_rows = []
    for sid in SERIES_IDS:
        print(f"  Fetching FRED series: {sid}")
        obs = fetch_series(sid)
        for o in obs:
            all_rows.append(
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
    df = pd.DataFrame(all_rows)
    print(f"Total FRED rows: {len(df)}")
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    table_name = "bronze.fred_macro_raw"
    if catalog.table_exists(table_name):
        catalog.load_table(table_name).append(arrow_table)
    else:
        tbl = catalog.create_table(
            table_name, schema=None, location="s3://lakehouse/bronze/fred_macro_raw"
        )
        tbl.append(arrow_table)
    print("Done.")


if __name__ == "__main__":
    run()

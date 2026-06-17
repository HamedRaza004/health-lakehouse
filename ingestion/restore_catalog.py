from pyiceberg.catalog import load_catalog
import os
from dotenv import load_dotenv

load_dotenv()

catalog = load_catalog(
    "default",
    **{
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    },
)

# recreate namespaces
for ns in ["bronze", "silver", "gold"]:
    try:
        catalog.create_namespace(ns)
        print(f"  Created namespace: {ns}")
    except Exception as e:
        print(f"  Namespace exists: {ns}")

# list what's actually in MinIO so we register exactly what's there
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)


def find_metadata_path(bucket: str, prefix: str) -> str | None:
    """Find the latest metadata JSON file for an Iceberg table."""
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{prefix}/metadata/",
    )
    if "Contents" not in resp:
        return None
    # get all version-hint or metadata JSON files
    meta_files = [
        obj["Key"]
        for obj in resp["Contents"]
        if obj["Key"].endswith(".json") and "version-hint" not in obj["Key"]
    ]
    if not meta_files:
        return None
    # return the latest one by LastModified
    latest = sorted(
        [obj for obj in resp["Contents"] if obj["Key"] in meta_files],
        key=lambda x: x["LastModified"],
        reverse=True,
    )[0]["Key"]
    return f"s3://lakehouse/{latest}"


# bronze tables — match exactly what you have in MinIO
BRONZE_TABLES = [
    ("bronze.who_gho_raw", "bronze/who_gho_raw"),
    ("bronze.cdc_nndss_raw", "bronze/cdc_nndss_raw"),
    ("bronze.owid_covid_raw", "bronze/owid_covid_raw"),
    ("bronze.owid_vaccination_raw", "bronze/owid_vaccination_raw"),
    ("bronze.fred_macro_raw", "bronze/fred_macro_raw"),
    ("bronze.openfda_drug_event_raw", "bronze/openfda_drug_event_raw"),
]

print("\n--- Registering bronze tables ---")
for table_name, prefix in BRONZE_TABLES:
    meta_path = find_metadata_path("lakehouse", prefix)
    if meta_path is None:
        print(f"  SKIP {table_name} — no metadata found at {prefix}/metadata/")
        continue
    try:
        catalog.register_table(table_name, meta_path)
        print(f"  OK   {table_name} → {meta_path}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  EXISTS {table_name}")
        else:
            print(f"  FAIL {table_name}: {e}")

print("\n--- Verifying registrations ---")
for table_name, _ in BRONZE_TABLES:
    exists = catalog.table_exists(table_name)
    if exists:
        count = catalog.load_table(table_name).scan().to_arrow().num_rows
        print(f"  {table_name}: {count:,} rows")
    else:
        print(f"  MISSING: {table_name}")

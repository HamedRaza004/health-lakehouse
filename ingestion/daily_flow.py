from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime, timezone
from pathlib import Path
import os

import who_gho
import cdc_nndss
import owid
import fred
import openfda

# ---------------------------------------------------------------------------
# Local cache directory — sits next to this file at runtime
# ---------------------------------------------------------------------------
CACHE_DIR = Path(os.getenv("INGESTION_CACHE_DIR", "/tmp/ingestion_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_TTL_HOURS = int(os.getenv("CACHE_TTL_HOURS", "20"))


def _cache_is_fresh(path: Path, ttl_hours: int = CACHE_TTL_HOURS) -> bool:
    """Return True if the cache file exists and is younger than ttl_hours."""
    if not path.exists():
        return False
    age = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age < ttl_hours * 3600


# ---------------------------------------------------------------------------
# FIX 3 — Safe bronze validation (no full table scan into memory)
# ---------------------------------------------------------------------------
def _safe_table_info(catalog, table_name: str, logger) -> None:
    """
    Log table existence and snapshot metadata only.
    Never calls .scan().to_arrow() — avoids loading entire table into RAM.
    """
    if not catalog.table_exists(table_name):
        logger.warning(f"MISSING TABLE: {table_name}")
        return

    tbl = catalog.load_table(table_name)
    snap = tbl.current_snapshot()

    if snap is None:
        logger.info(f"{table_name}: table exists but has no snapshots yet")
        return

    # Summary metrics come from Iceberg snapshot summary — zero data loaded
    summary = snap.summary or {}
    added_rows = summary.get("added-records", "?")
    total_records = summary.get("total-records", "?")
    added_files = summary.get("added-data-files", "?")
    total_files = summary.get("total-data-files", "?")

    logger.info(
        f"{table_name}: snapshot {snap.snapshot_id} | "
        f"total_records={total_records} | added_records={added_rows} | "
        f"total_files={total_files} | added_files={added_files}"
    )


# ---------------------------------------------------------------------------
# Helper — FIX 2: drop-and-recreate so daily runs never duplicate rows
# ---------------------------------------------------------------------------
def _replace_table(catalog, table_name: str, logger) -> None:
    """
    Drop the bronze table if it exists so the ingestion script can recreate it
    fresh.  This eliminates the append-duplication problem for sources that
    deliver full historical snapshots on every fetch (OWID, CDC, FDA).
    """
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)
        logger.info(f"Dropped existing table {table_name} — will recreate fresh")


def _get_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
            "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "s3.path-style-access": "true",
        },
    )


# ---------------------------------------------------------------------------
# Ingestion tasks
# ---------------------------------------------------------------------------

# FIX 3 — cache_key_fn + cache_expiration applied to ALL heavy sources
_heavy_task_kwargs = dict(
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=CACHE_TTL_HOURS),
)


@task(name="ingest-who-gho", **_heavy_task_kwargs)
def ingest_who():
    logger = get_run_logger()
    sentinel = CACHE_DIR / "who_gho.done"

    # Cache guard — skip the API call if we ran successfully within TTL
    if _cache_is_fresh(sentinel):
        logger.info(f"WHO GHO: ran recently (sentinel fresh), skipping fetch")
        return

    logger.info("WHO GHO: fetching from API")
    catalog = _get_catalog()
    _replace_table(catalog, "bronze.who_gho_raw", logger)
    who_gho.run()  # ← unchanged — no new args
    sentinel.touch()  # mark success for next TTL window
    logger.info("WHO GHO ingestion complete")


@task(name="ingest-cdc-nndss", **_heavy_task_kwargs)
def ingest_cdc():
    logger = get_run_logger()
    sentinel = CACHE_DIR / "cdc_nndss.done"

    if _cache_is_fresh(sentinel):
        logger.info("CDC NNDSS: ran recently, skipping fetch")
        return

    logger.info("CDC NNDSS: fetching from API")
    catalog = _get_catalog()
    _replace_table(catalog, "bronze.cdc_nndss_raw", logger)
    cdc_nndss.run()  # ← unchanged
    sentinel.touch()
    logger.info("CDC NNDSS ingestion complete")


@task(name="ingest-owid", **_heavy_task_kwargs)
def ingest_owid():
    logger = get_run_logger()
    sentinel = CACHE_DIR / "owid.done"

    if _cache_is_fresh(sentinel):
        logger.info("OWID: ran recently, skipping fetch")
        return

    logger.info("OWID: fetching from API")
    catalog = _get_catalog()
    _replace_table(catalog, "bronze.owid_covid_raw", logger)
    _replace_table(catalog, "bronze.owid_vaccination_raw", logger)
    owid.run()  # ← unchanged
    sentinel.touch()
    logger.info("OWID ingestion complete")


# FRED is incremental — append is correct, no drop-and-recreate needed
@task(name="ingest-fred", retries=3, retry_delay_seconds=60)
def ingest_fred():
    logger = get_run_logger()
    logger.info("Starting FRED ingestion")
    fred.run()  # ← unchanged
    logger.info("FRED ingestion complete")


@task(name="ingest-openfda", **_heavy_task_kwargs)
def ingest_openfda():
    logger = get_run_logger()
    sentinel = CACHE_DIR / "openfda.done"

    if _cache_is_fresh(sentinel):
        logger.info("OpenFDA: ran recently, skipping fetch")
        return

    logger.info("OpenFDA: fetching from API")
    catalog = _get_catalog()
    _replace_table(catalog, "bronze.fda_adverse_events_raw", logger)
    openfda.run()  # ← unchanged
    sentinel.touch()
    logger.info("OpenFDA ingestion complete")


@task(name="validate-bronze-counts")
def validate_bronze_counts():
    """
    FIX 1 — Safe validation using Iceberg snapshot metadata only.
    No .scan().to_arrow() — no data loaded into memory.
    """
    catalog = _get_catalog()
    logger = get_run_logger()
    tables = [
        "bronze.who_gho_raw",
        "bronze.cdc_nndss_raw",
        "bronze.owid_covid_raw",
        "bronze.owid_vaccination_raw",
        "bronze.fred_macro_raw",
        "bronze.fda_adverse_events_raw",
    ]
    for t in tables:
        _safe_table_info(catalog, t, logger)


# ---------------------------------------------------------------------------
# Flow — FIX 4: safer parallelism strategy
# ---------------------------------------------------------------------------


@flow(
    name="daily-health-ingestion",
    description="Ingest all public health sources into bronze Iceberg tables",
)
def daily_ingestion_flow():
    logger = get_run_logger()
    logger.info("Starting daily ingestion flow")

    # FIX 4 — Parallelism strategy:
    #
    #   PARALLEL  : WHO + FRED  — lightweight payloads, low RAM, low rate-limit risk
    #   SEQUENTIAL: CDC → OWID → OpenFDA — large downloads, run one at a time
    #               to avoid RAM exhaustion and simultaneous API rate-limit hits
    #
    who_future = ingest_who.submit()
    fred_future = ingest_fred.submit()

    # Wait for the light tasks to finish first
    who_future.result(raise_on_failure=False)
    fred_future.result(raise_on_failure=False)

    # Heavy tasks run sequentially to protect memory and API quotas
    ingest_cdc()
    ingest_owid()
    ingest_openfda()

    validate_bronze_counts()
    logger.info("Daily ingestion flow complete")


if __name__ == "__main__":
    daily_ingestion_flow()

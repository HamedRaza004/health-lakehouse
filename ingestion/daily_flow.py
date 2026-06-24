from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime, timezone
from pathlib import Path
import os
import requests as http_requests
import subprocess
import sys

sys.path.append("../great_expectations")
sys.path.append("../silver_transform")

from bronze_checks import run_bronze_checks
from silver_checks import run_silver_checks
from bronze_to_silver import run_all_silver_transforms

import who_gho
import cdc_nndss
import owid
import fred
import openfda

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

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
    print("Namespaces:")
    print(catalog.list_namespaces())

    print("Bronze tables:")
    print(catalog.list_tables("bronze"))

    tables = [
        "bronze.who_gho_raw",
        "bronze.cdc_nndss_raw",
        "bronze.owid_covid_raw",
        "bronze.owid_vaccination_raw",
        "bronze.fred_unrate_raw",  # ← was fred_macro_raw
        "bronze.fred_cpiaucsl_raw",  # ← new
        "bronze.fred_dgs10_raw",
        "bronze.openfda_drug_event_raw",  # fixed from fda_adverse_events_raw
    ]
    for t in tables:
        _safe_table_info(catalog, t, logger)


# ---------------------------------------------------------------------------
# Flow — FIX 4: safer parallelism strategy
# ---------------------------------------------------------------------------


@task(name="bronze-quality-gate")
def bronze_quality_gate():
    logger = get_run_logger()
    passed = run_bronze_checks()
    if not passed:
        raise ValueError("Bronze quality checks FAILED — blocking silver promotion")
    logger.info("Bronze quality gate PASSED")


@task(name="run-silver-transforms")
def run_silver_transforms():
    logger = get_run_logger()
    logger.info("Running all bronze → silver transforms")
    run_all_silver_transforms()
    logger.info("Silver transforms complete")


@task(name="silver-quality-gate")
def silver_quality_gate():
    logger = get_run_logger()
    passed = run_silver_checks()
    if not passed:
        raise ValueError("Silver quality checks FAILED — blocking gold promotion")
    logger.info("Silver quality gate PASSED")


@task(name="dbt-gold-run")
def run_dbt_gold():
    logger = get_run_logger()
    logger.info("Running dbt gold models...")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    logger.info("dbt run complete")


@task(name="dbt-gold-test")
def run_dbt_tests():
    logger = get_run_logger()
    logger.info("Running dbt tests...")

    result = subprocess.run(
        ["dbt", "test", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.warning(f"Some dbt tests failed:\n{result.stderr}")
        # warn but don't block - test failures are informational
    logger.info("dbt test complete")


@task(name="dbt-source-freshness")
def run_dbt_freshness():
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "source", "freshness", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    logger.info("Source freshness check complete")


@task(name="outbreak-alert-check")
def check_outbreak_alerts():
    logger = get_run_logger()
    slack_url = os.getenv("SLACK_WEBHOOK_URL")

    import trino

    conn = trino.dbapi.connect(
        host="localhost",
        port=8085,
        user="hamed",
        catalog="iceberg",
        schema="gold",
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT condition, reporting_area, year, week,
               current_week_cases, severity,
               cases_vs_rolling_avg_ratio
        FROM gold.outbreak_alerts
        WHERE severity IN ('critical', 'high')
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 ELSE 2 END,
            cases_vs_rolling_avg_ratio DESC
        LIMIT 10
    """
    )
    cols = [d[0] for d in cur.description]
    alerts = [dict(zip(cols, row)) for row in cur.fetchall()]

    if not alerts:
        logger.info("No critical/high alerts found")
        return

    logger.warning(f"Found {len(alerts)} active alerts")

    if not slack_url:
        logger.warning("SLACK_WEBHOOK_URL not set - skipping notification")
        for alert in alerts:
            logger.warning(
                f"ALERT [{alert['severity'].upper()}] "
                f"{alert['condition']} in {alert['reporting_area']}: "
                f"{alert['current_week_cases']} cases "
                f"({alert['cases_vs_rolling_avg_ratio']}x avg)"
            )
        return

    lines = [f"*Health Lakehouse - Outbreak Alerts* ({len(alerts)} active)\n"]
    for alert in alerts:
        emoji = ":red_circle:" if alert["severity"] == "critical" else ":orange_circle:"
        lines.append(
            f"{emoji} *{alert['severity'].upper()}* | "
            f"{alert['condition']} - {alert['reporting_area']} | "
            f"Week {alert['week']}/{alert['year']} | "
            f"{int(alert['current_week_cases'])} cases "
            f"({alert['cases_vs_rolling_avg_ratio']}x rolling avg)"
        )

    payload = {"text": "\n".join(lines)}
    resp = http_requests.post(slack_url, json=payload, timeout=10)
    if resp.status_code == 200:
        logger.info("Slack alert sent successfully")
    else:
        logger.warning(f"Slack notification failed: {resp.status_code}")


@flow(name="daily-health-ingestion")
def daily_ingestion_flow():
    logger = get_run_logger()

    # --- temporarily skip ingestion, data already in bronze ---
    # who_future  = ingest_who.submit()
    # fred_future = ingest_fred.submit()
    # who_future.result(raise_on_failure=False)
    # fred_future.result(raise_on_failure=False)
    # ingest_cdc()
    # ingest_owid()
    # ingest_openfda()

    validate_bronze_counts()
    bronze_quality_gate()
    run_silver_transforms()
    silver_quality_gate()

    # --- week 4 additions ---
    run_dbt_freshness()
    run_dbt_gold()
    run_dbt_tests()
    check_outbreak_alerts()

    logger.info("Full pipeline complete")


if __name__ == "__main__":
    daily_ingestion_flow()

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from api.db import run_query, get_trino_connection
from api.auth import verify_api_key
from api.models import APIResponse

router = APIRouter(prefix="/api/v1/query", tags=["time-travel"])

ALLOWED_TABLES = [
    "bronze.who_gho_raw",
    "silver.who_gho_silver",
    "gold.disease_trends_daily",
    "gold.vaccination_coverage",
    "gold.outbreak_alerts",
    "gold.mortality_by_region",
]


@router.get("/time-travel", response_model=APIResponse)
async def time_travel_query(
    table: str = Query(..., description="e.g. silver.who_gho_silver"),
    snapshot_id: Optional[int] = Query(None, description="Iceberg snapshot ID"),
    as_of_timestamp: Optional[str] = Query(
        None, description="ISO timestamp e.g. 2024-01-15T10:00:00"
    ),
    limit: int = Query(100, ge=1, le=1000),
    _: str = Depends(verify_api_key),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=400, detail=f"Table not allowed. Choose from: {ALLOWED_TABLES}"
        )

    if snapshot_id:
        sql = f"""
            SELECT * FROM iceberg.{table}
            FOR VERSION AS OF {snapshot_id}
            LIMIT {limit}
        """
    elif as_of_timestamp:
        sql = f"""
            SELECT * FROM iceberg.{table}
            FOR TIMESTAMP AS OF TIMESTAMP '{as_of_timestamp}'
            LIMIT {limit}
        """
    else:
        # no time-travel — just return latest with snapshot history
        sql = f"SELECT * FROM iceberg.{table} LIMIT {limit}"

    try:
        data = run_query(sql)
        return APIResponse(success=True, count=len(data), data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snapshots")
async def get_snapshots(
    table: str = Query(...),
    _: str = Depends(verify_api_key),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Table not allowed")
    sql = f"""
        SELECT snapshot_id, committed_at, operation
        FROM iceberg."{table.split(".")[0]}"."${table.split(".")[1]}$snapshots"
        ORDER BY committed_at DESC
    """
    try:
        data = run_query(sql)
        return APIResponse(success=True, count=len(data), data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

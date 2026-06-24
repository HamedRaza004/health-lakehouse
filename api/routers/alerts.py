from fastapi import APIRouter, Depends, Query
from typing import Optional
from api.db import run_query
from api.auth import verify_api_key
from api.models import APIResponse

router = APIRouter(prefix="/api/v1/outbreaks", tags=["outbreaks"])

VALID_SEVERITIES = ["critical", "high", "medium", "watch"]

@router.get("/alerts", response_model=APIResponse)
async def get_outbreak_alerts(
    severity:       Optional[str] = Query(None),
    condition:      Optional[str] = Query(None),
    reporting_area: Optional[str] = Query(None),
    limit:          int           = Query(50, ge=1, le=1000),
    _: str = Depends(verify_api_key),
):
    filters = []
    if severity:
        filters.append(f"severity = '{severity}'")
    if condition:
        filters.append(f"LOWER(condition) LIKE '%{condition.lower()}%'")
    if reporting_area:
        filters.append(
            f"LOWER(reporting_area) LIKE '%{reporting_area.lower()}%'"
        )

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT condition, reporting_area, year, week,
               current_week_cases, previous_52_week_max,
               cases_vs_rolling_avg_ratio, severity, is_outbreak
        FROM gold.outbreak_alerts
        {where}
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1
                WHEN 'high'     THEN 2
                WHEN 'medium'   THEN 3
                ELSE 4
            END,
            cases_vs_rolling_avg_ratio DESC
        LIMIT {limit}
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)


@router.get("/summary")
async def get_alerts_summary(_: str = Depends(verify_api_key)):
    sql = """
        SELECT severity, COUNT(*) as alert_count
        FROM gold.outbreak_alerts
        GROUP BY severity
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1
                WHEN 'high'     THEN 2
                WHEN 'medium'   THEN 3
                ELSE 4
            END
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)
from fastapi import APIRouter, Depends, Query
from typing import Optional
from api.db import run_query
from api.auth import verify_api_key
from api.models import APIResponse

router = APIRouter(prefix="/api/v1/diseases", tags=["diseases"])


@router.get("/trend", response_model=APIResponse)
async def get_disease_trend(
    indicator_code: Optional[str] = Query(None),
    country_code: Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    _: str = Depends(verify_api_key),
):
    filters = []
    if indicator_code:
        filters.append(f"indicator_code = '{indicator_code.upper()}'")
    if country_code:
        filters.append(f"country_code = '{country_code.upper()}'")
    if year_from:
        filters.append(f"year >= {year_from}")
    if year_to:
        filters.append(f"year <= {year_to}")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT indicator_code, indicator_name, country_code,
               country_name, year, sex, metric_value,
               rolling_3yr_avg, deviation_from_avg
        FROM gold.disease_trends_daily
        {where}
        ORDER BY year DESC, metric_value DESC
        LIMIT {limit}
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)


@router.get("/indicators")
async def list_indicators(_: str = Depends(verify_api_key)):
    sql = """
        SELECT DISTINCT indicator_code, indicator_name
        FROM gold.disease_trends_daily
        ORDER BY indicator_code
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)


@router.get("/countries")
async def list_countries(_: str = Depends(verify_api_key)):
    sql = """
        SELECT DISTINCT country_code, country_name
        FROM gold.disease_trends_daily
        WHERE country_code IS NOT NULL
        ORDER BY country_code
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)

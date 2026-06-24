from fastapi import APIRouter, Depends, Query
from typing import Optional
from api.db import run_query
from api.auth import verify_api_key
from api.models import APIResponse

router = APIRouter(prefix="/api/v1/vaccination", tags=["vaccination"])


@router.get("/coverage", response_model=APIResponse)
async def get_vaccination_coverage(
    iso_code: Optional[str] = Query(None),
    category: Optional[str] = Query(None, description="high | medium | low | very_low"),
    limit: int = Query(100, ge=1, le=1000),
    _: str = Depends(verify_api_key),
):
    filters = []
    if iso_code:
        filters.append(f"iso_code = '{iso_code.upper()}'")
    if category:
        filters.append(f"coverage_category = '{category}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT iso_code, location, latest_date,
               pct_at_least_one_dose,
               pct_fully_vaccinated,
               coverage_category,
               total_vaccinations
        FROM gold.vaccination_coverage
        {where}
        ORDER BY pct_fully_vaccinated DESC
        LIMIT {limit}
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)


@router.get("/summary")
async def get_coverage_summary(_: str = Depends(verify_api_key)):
    sql = """
        SELECT coverage_category,
               COUNT(*) as country_count,
               ROUND(AVG(pct_fully_vaccinated), 2) as avg_pct_vaccinated
        FROM gold.vaccination_coverage
        GROUP BY coverage_category
        ORDER BY avg_pct_vaccinated DESC
    """
    data = run_query(sql)
    return APIResponse(success=True, count=len(data), data=data)

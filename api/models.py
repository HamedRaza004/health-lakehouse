from pydantic import BaseModel, Field
from typing import Optional

class DiseaseTrendParams(BaseModel):
    country: Optional[str] = None
    year_from: Optional[int] = Field(None, ge=1900, le=2100)
    year_to: Optional[int] = Field(None, ge=1900, le=2100)
    limit: int = Field(100, ge=1, le=10000)

class VaccinationParams(BaseModel):
    iso_code: Optional[str] = None
    category: Optional[str] = None

class AlertParams(BaseModel):
    severity: Optional[str] = None
    condition: Optional[str] = None
    reporting_area: Optional[str] = None
    limit: int = Field(50, ge=1, le=1000)

class TimeTravelParams(BaseModel):
    table: str
    snapshot_id: Optional[int] = None
    as_of_timestamp: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)

class APIResponse(BaseModel):
    success: bool
    count: int
    data: list[dict]
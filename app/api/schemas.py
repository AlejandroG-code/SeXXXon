import uuid
from datetime import date, datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, model_validator


class ReportCreateRequest(BaseModel):
    report_type: str = Field(default="sales", description="Type of report. Currently supported: 'sales'")
    start_date: date = Field(..., description="Start date (inclusive) YYYY-MM-DD")
    end_date: date = Field(..., description="End date (inclusive) YYYY-MM-DD")
    simulate_failure: Optional[Literal["transient", "permanent"]] = Field(
        default=None,
        description="Test hook to simulate transient or permanent worker failure"
    )

    @model_validator(mode="after")
    def validate_dates_and_type(self) -> "ReportCreateRequest":
        if self.report_type.lower() != "sales":
            raise ValueError(f"Unsupported report type '{self.report_type}'. Supported: 'sales'")
        if self.start_date > self.end_date:
            raise ValueError(f"Start date ({self.start_date}) cannot be after end date ({self.end_date})")
        return self


class ReportResponse(BaseModel):
    report_id: uuid.UUID
    status: str
    status_url: str
    download_url: Optional[str] = None
    report_type: str
    parameters: dict
    attempts: int
    row_count: Optional[int] = None
    file_size_bytes: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    worker_id: Optional[str] = None
    error: Optional[str] = None
    error_category: Optional[str] = None


class ReportAcceptedResponse(BaseModel):
    report_id: uuid.UUID
    status: str
    status_url: str
    message: str = "Report request accepted"


class ReconcileResponse(BaseModel):
    reconciled_count: int
    reconciled_ids: list[str]

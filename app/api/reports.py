import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import update

from app.api.schemas import (
    ReportCreateRequest,
    ReportResponse,
    ReportAcceptedResponse,
    ReconcileResponse,
)
from app.broker.rabbitmq import publish_job_message
from app.core.config import settings
from app.core.logging import logger
from app.db.models import Report
from app.db.session import get_db

router = APIRouter(prefix="/reports", tags=["Reports"])


def build_report_response(report: Report) -> ReportResponse:
    storage_path = settings.get_storage_path()
    download_url = f"/reports/{report.id}/download" if report.status == "completed" else None

    return ReportResponse(
        report_id=report.id,
        status=report.status,
        status_url=f"/reports/{report.id}",
        download_url=download_url,
        report_type=report.report_type,
        parameters=report.parameters,
        attempts=report.attempts,
        row_count=report.row_count,
        file_size_bytes=report.file_size_bytes,
        created_at=report.created_at,
        updated_at=report.updated_at,
        queued_at=report.queued_at,
        started_at=report.started_at,
        completed_at=report.completed_at,
        worker_id=report.worker_id,
        error=report.error_message,
        error_category=report.error_category,
    )


@router.post("", response_model=ReportAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def create_report(
    payload: ReportCreateRequest,
    response: Response,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    db: Session = Depends(get_db),
):
    """
    Accept a report request asynchronously:
    1. Validate parameters and idempotency key.
    2. Persist record in database (pending).
    3. Publish to RabbitMQ with publisher confirms.
    4. Return 202 Accepted immediately.
    """
    # 1. Idempotency Check
    if idempotency_key:
        existing = db.query(Report).filter(Report.idempotency_key == idempotency_key).first()
        if existing:
            # Check if parameters match
            req_params = {
                "report_type": payload.report_type.lower(),
                "start_date": str(payload.start_date),
                "end_date": str(payload.end_date),
            }
            existing_params = {
                "report_type": existing.parameters.get("report_type", "").lower(),
                "start_date": str(existing.parameters.get("start_date", "")),
                "end_date": str(existing.parameters.get("end_date", "")),
            }

            if req_params == existing_params:
                # Idempotent match: return existing report
                response.status_code = status.HTTP_200_OK
                return ReportAcceptedResponse(
                    report_id=existing.id,
                    status=existing.status,
                    status_url=f"/reports/{existing.id}",
                    message="Existing report request reused (idempotency key matched).",
                )
            else:
                # Key reused with different parameters -> 409 Conflict
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key already used with different report parameters.",
                )

    # 2. Persist new report in pending state
    report_id = uuid.uuid4()
    report_params = {
        "report_type": payload.report_type.lower(),
        "start_date": str(payload.start_date),
        "end_date": str(payload.end_date),
    }
    if payload.simulate_failure:
        report_params["simulate_failure"] = payload.simulate_failure

    new_report = Report(
        id=report_id,
        idempotency_key=idempotency_key,
        report_type=payload.report_type.lower(),
        parameters=report_params,
        status="pending",
        attempts=0,
    )

    try:
        db.add(new_report)
        db.commit()
        db.refresh(new_report)
    except IntegrityError:
        db.rollback()
        # Concurrent request with same idempotency key
        existing = db.query(Report).filter(Report.idempotency_key == idempotency_key).first()
        if existing:
            return ReportAcceptedResponse(
                report_id=existing.id,
                status=existing.status,
                status_url=f"/reports/{existing.id}",
                message="Existing report request reused (concurrent idempotency key matched).",
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Conflict creating report record.",
        )

    # 3. Publish to RabbitMQ using Publisher Confirms
    published = publish_job_message(
        report_id=str(report_id),
        attempt=1,
        simulate_failure=payload.simulate_failure,
    )

    if published:
        db.execute(
            update(Report)
            .where(Report.id == new_report.id, Report.status == "pending")
            .values(status="queued", queued_at=datetime.now(timezone.utc))
        )
        db.commit()
        db.refresh(new_report)
    else:
        # Broker was unavailable or unroutable: keep report in 'pending' status!
        # Do NOT silently lose the job; reconciliation command can republish it.
        logger.warning(
            f"Broker publish failed for report {report_id}. Retaining pending record for reconciliation.",
            extra={"report_id": str(report_id), "status": "pending", "action": "publish_failed"},
        )

    return ReportAcceptedResponse(
        report_id=new_report.id,
        status=new_report.status,
        status_url=f"/reports/{new_report.id}",
        message="Report generation request accepted.",
    )


@router.get("", response_model=list[ReportResponse])
def list_reports(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List recent reports (primarily used for the frontend dashboard)."""
    reports = db.query(Report).order_by(Report.created_at.desc()).limit(limit).all()
    return [build_report_response(r) for r in reports]


@router.get("/{report_id}", response_model=ReportResponse)
def get_report_status(
    report_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """
    Get current report metadata and lifecycle status:
    - pending, queued, processing, retrying, completed, failed
    - attempts count
    - row count and download URL when completed
    - safe error message when failed
    """
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report with ID '{report_id}' was not found.",
        )

    return build_report_response(report)


@router.get("/{report_id}/download")
def download_report(
    report_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """
    Download the generated CSV file when completed:
    - 404 for unknown report ID
    - 409 Conflict if report is not ready (pending, queued, processing, retrying, failed)
    - Strict filesystem path validation preventing arbitrary directory traversal
    """
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report with ID '{report_id}' was not found.",
        )

    if report.status != "completed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Report is not ready for download. Current status is '{report.status}'.",
        )

    storage_path = settings.get_storage_path()
    file_path = (storage_path / f"{report.id}.csv").resolve()

    # Prevent directory traversal: verify resolved path is inside storage_path
    if not str(file_path).startswith(str(storage_path)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to specified file path is forbidden.",
        )

    if not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report file is missing from storage.",
        )

    return FileResponse(
        path=str(file_path),
        media_type="text/csv",
        filename=f"sales_report_{report.id}.csv",
        headers={
            "Content-Disposition": f'attachment; filename="sales_report_{report.id}.csv"'
        },
    )


@router.post("/reconcile", response_model=ReconcileResponse)
def reconcile_pending_reports(db: Session = Depends(get_db)):
    """
    Reconciliation endpoint:
    Finds reports stuck in 'pending' status (e.g. from broker downtime) and publishes them.
    """
    pending_reports = db.query(Report).filter(Report.status == "pending").all()
    reconciled_ids = []

    for report in pending_reports:
        sim_failure = report.parameters.get("simulate_failure")
        published = publish_job_message(
            report_id=str(report.id),
            attempt=1,
            simulate_failure=sim_failure,
        )
        if published:
            report.status = "queued"
            report.queued_at = datetime.now(timezone.utc)
            reconciled_ids.append(str(report.id))

    if reconciled_ids:
        db.commit()

    return ReconcileResponse(
        reconciled_count=len(reconciled_ids),
        reconciled_ids=reconciled_ids,
    )

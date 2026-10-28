import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    Index,
    JSON,
)
from sqlalchemy.dialects.postgresql import UUID
from app.db.session import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Sale(Base):
    __tablename__ = "sales"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    sale_date = Column(Date, nullable=False, index=True)
    customer_name = Column(String(255), nullable=False)
    product_name = Column(String(255), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    total_amount = Column(Numeric(10, 2), nullable=False)

    __table_args__ = (
        Index("idx_sales_date_id", "sale_date", "id"),
    )


class Report(Base):
    __tablename__ = "reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(String(255), unique=True, nullable=True, index=True)
    report_type = Column(String(50), nullable=False, default="sales")
    parameters = Column(JSON, nullable=False)

    # Job lifecycle statuses: pending, queued, processing, retrying, completed, failed
    status = Column(String(30), nullable=False, default="pending", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    row_count = Column(Integer, nullable=True)

    file_path = Column(String(500), nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)

    error_message = Column(Text, nullable=True)
    error_category = Column(String(50), nullable=True)
    worker_id = Column(String(100), nullable=True)

    # Lifecycle Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)
    queued_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_reports_status_created", "status", "created_at"),
    )

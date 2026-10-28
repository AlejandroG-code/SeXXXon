import csv
import json
import os
import signal
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pika
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from app.broker.rabbitmq import (
    get_rabbitmq_connection,
    setup_rabbitmq_topology,
    publish_retry_message,
    publish_dlq_message,
)
from app.core.config import settings
from app.core.logging import logger
from app.db.models import Report, Sale
from app.db.session import SessionLocal, engine


class TransientProcessingError(Exception):
    """Temporary failure that should be retried."""
    pass


class PermanentProcessingError(Exception):
    """Unrecoverable failure that should be sent directly to DLQ."""
    pass


class ReportWorker:
    def __init__(self, worker_id: Optional[str] = None):
        self.worker_id = worker_id or os.getenv("WORKER_ID", f"worker-{os.getpid()}")
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.is_running = True

        # Register graceful shutdown signals
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        logger.info(
            f"Worker {self.worker_id} received shutdown signal ({signum}). Stopping gracefully...",
            extra={"worker_id": self.worker_id, "action": "worker_shutdown"},
        )
        self.is_running = False
        if self.channel and self.channel.is_open:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass

    def run(self) -> None:
        """Start worker consumer loop with auto-reconnect."""
        logger.info(
            f"Starting worker {self.worker_id} consuming from {settings.QUEUE_NAME} (prefetch=1)...",
            extra={"worker_id": self.worker_id, "action": "worker_start"},
        )

        while self.is_running:
            try:
                self.connection = get_rabbitmq_connection()
                self.channel = self.connection.channel()
                setup_rabbitmq_topology(self.channel)

                # Crucial for distributing work evenly among multiple workers
                self.channel.basic_qos(prefetch_count=1)

                self.channel.basic_consume(
                    queue=settings.QUEUE_NAME,
                    on_message_callback=self.process_message,
                    auto_ack=False,  # Manual acknowledgments required
                )

                logger.info(
                    f"Worker {self.worker_id} successfully connected and listening for jobs.",
                    extra={"worker_id": self.worker_id, "action": "listening"},
                )
                self.channel.start_consuming()

            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker) as exc:
                if not self.is_running:
                    break
                logger.warning(
                    f"Worker {self.worker_id} disconnected from broker: {exc}. Reconnecting in 3s...",
                    extra={"worker_id": self.worker_id, "action": "reconnecting", "error_category": "broker_disconnect"},
                )
                time.sleep(3)
            except Exception as exc:
                if not self.is_running:
                    break
                logger.error(
                    f"Worker {self.worker_id} unexpected error: {exc}. Reconnecting in 5s...",
                    extra={"worker_id": self.worker_id, "action": "worker_error", "error_category": "fatal_loop"},
                    exc_info=True,
                )
                time.sleep(5)

    def process_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """
        Process a single report generation message:
        1. Atomic claim in PostgreSQL.
        2. Deduplication check (skip if already completed).
        3. Configurable delay + sales CSV generation.
        4. Atomic file rename (temp -> final).
        5. PostgreSQL status update to completed.
        6. Manual basic_ack only after file and DB state are committed.
        """
        try:
            payload = json.loads(body.decode("utf-8"))
            report_id = payload.get("report_id")
            message_attempt = payload.get("attempt", 1)
            simulate_failure = payload.get("simulate_failure")
        except Exception as exc:
            logger.error(
                f"Malformed message body received: {exc}",
                extra={"worker_id": self.worker_id, "action": "malformed_message", "error_category": "format_error"},
            )
            # Cannot process malformed message, reject without requeue
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(
            f"Worker {self.worker_id} received message for report {report_id} (attempt {message_attempt})",
            extra={
                "report_id": report_id,
                "worker_id": self.worker_id,
                "attempt_number": message_attempt,
                "action": "message_received",
            },
        )

        db: Session = SessionLocal()
        temp_file_path: Optional[Path] = None

        try:
            # 1. Inspect report state in PostgreSQL
            report = db.query(Report).filter(Report.id == report_id).first()
            if not report:
                logger.warning(
                    f"Report {report_id} not found in database. Rejecting message.",
                    extra={"report_id": report_id, "worker_id": self.worker_id, "action": "report_not_found"},
                )
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Deduplication: If already completed, do not re-process!
            if report.status == "completed":
                logger.info(
                    f"Report {report_id} is already completed. Skipping processing and acknowledging duplicate.",
                    extra={
                        "report_id": report_id,
                        "worker_id": self.worker_id,
                        "status": "completed",
                        "action": "duplicate_skipped",
                    },
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Atomic status update to processing and increment attempt count
            current_attempt = (report.attempts or 0) + 1
            report.status = "processing"
            report.worker_id = self.worker_id
            report.attempts = current_attempt
            report.started_at = datetime.now(timezone.utc)
            report.updated_at = datetime.now(timezone.utc)
            db.commit()

            logger.info(
                f"Worker {self.worker_id} claimed report {report_id} (attempt {current_attempt})",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "attempt_number": current_attempt,
                    "status": "processing",
                    "action": "job_claimed",
                },
            )

            # 2. Check for Simulated Failure (Test scenario hooks)
            if simulate_failure == "transient":
                # Fails only on the first attempt, succeeds on retry
                if current_attempt == 1:
                    raise TransientProcessingError("Simulated transient connection timeout during report generation")
            elif simulate_failure == "permanent":
                # Fails permanently across all attempts
                raise PermanentProcessingError("Simulated permanent corrupt data format encountered")

            # 3. Configurable delay for demonstration (does not block API)
            delay = settings.WORKER_PROCESSING_DELAY_SECONDS
            if delay > 0:
                logger.info(
                    f"Worker {self.worker_id} applying processing delay ({delay}s)...",
                    extra={
                        "report_id": report_id,
                        "worker_id": self.worker_id,
                        "attempt_number": current_attempt,
                        "action": "processing_delay",
                    },
                )
                time.sleep(delay)

            # 4. Extract parameters and query sales dataset
            params = report.parameters or {}
            start_date_val = params.get("start_date")
            end_date_val = params.get("end_date")

            sales_query = db.query(Sale)
            if start_date_val:
                start_date_obj = date.fromisoformat(start_date_val) if isinstance(start_date_val, str) else start_date_val
                sales_query = sales_query.filter(Sale.sale_date >= start_date_obj)
            if end_date_val:
                end_date_obj = date.fromisoformat(end_date_val) if isinstance(end_date_val, str) else end_date_val
                sales_query = sales_query.filter(Sale.sale_date <= end_date_obj)

            # Consistent ordering: sale_date followed by sale ID
            sales_records = sales_query.order_by(Sale.sale_date.asc(), Sale.id.asc()).all()

            # 5. Write to temporary CSV file first (temporary files must not be downloadable)
            storage_path = settings.get_storage_path()
            temp_filename = f".tmp_{report_id}_{self.worker_id}_{os.getpid()}.csv"
            temp_file_path = storage_path / temp_filename
            canonical_filename = f"{report_id}.csv"
            canonical_file_path = storage_path / canonical_filename

            row_count = 0
            with open(temp_file_path, mode="w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                # Header row
                writer.writerow([
                    "Sale ID",
                    "Sale Date",
                    "Customer Name",
                    "Product Name",
                    "Quantity",
                    "Unit Price",
                    "Total Amount",
                ])

                for sale in sales_records:
                    writer.writerow([
                        sale.id,
                        sale.sale_date.isoformat(),
                        sale.customer_name,
                        sale.product_name,
                        sale.quantity,
                        f"{sale.unit_price:.2f}",
                        f"{sale.total_amount:.2f}",
                    ])
                    row_count += 1

            # 6. Atomic file move to canonical path
            os.replace(temp_file_path, canonical_file_path)
            file_size = canonical_file_path.stat().st_size

            # 7. Update report state in PostgreSQL
            # Prevent delayed updates from moving a completed report back to an earlier status
            report.status = "completed"
            report.row_count = row_count
            report.file_path = str(canonical_file_path)
            report.file_size_bytes = file_size
            report.completed_at = datetime.now(timezone.utc)
            report.updated_at = datetime.now(timezone.utc)
            report.error_message = None
            report.error_category = None
            db.commit()

            # 8. Manual acknowledgment only AFTER file and DB commits succeed!
            channel.basic_ack(delivery_tag=method.delivery_tag)

            logger.info(
                f"Worker {self.worker_id} completed report {report_id} with {row_count} rows ({file_size} bytes)",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "attempt_number": current_attempt,
                    "status": "completed",
                    "action": "report_completed",
                },
            )

        except TransientProcessingError as exc:
            # Clean up temp file
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                except Exception:
                    pass

            logger.warning(
                f"Worker {self.worker_id} encountered transient error for report {report_id}: {exc}",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "attempt_number": current_attempt,
                    "status": "retrying",
                    "error_category": "transient",
                },
            )

            if current_attempt < settings.MAX_ATTEMPTS:
                # Bounded retry: update DB to retrying, publish to retry queue with delay, then ack current message
                report.status = "retrying"
                report.error_message = str(exc)
                report.error_category = "transient"
                report.updated_at = datetime.now(timezone.utc)
                db.commit()

                publish_retry_message(
                    report_id=str(report_id),
                    attempt=current_attempt + 1,
                    simulate_failure=simulate_failure,
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # Attempts exhausted: mark failed and send to DLQ
                report.status = "failed"
                report.error_message = f"Max attempts ({settings.MAX_ATTEMPTS}) reached. Last error: {exc}"
                report.error_category = "transient_exhausted"
                report.updated_at = datetime.now(timezone.utc)
                db.commit()

                publish_dlq_message(
                    report_id=str(report_id),
                    attempt=current_attempt,
                    error_message=report.error_message,
                    error_category="transient_exhausted",
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)

        except PermanentProcessingError as exc:
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                except Exception:
                    pass

            logger.error(
                f"Worker {self.worker_id} encountered permanent error for report {report_id}: {exc}",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "attempt_number": current_attempt,
                    "status": "failed",
                    "error_category": "permanent",
                },
            )

            report.status = "failed"
            report.error_message = str(exc)
            report.error_category = "permanent"
            report.updated_at = datetime.now(timezone.utc)
            db.commit()

            publish_dlq_message(
                report_id=str(report_id),
                attempt=current_attempt,
                error_message=str(exc),
                error_category="permanent",
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except OperationalError as exc:
            # Database unavailable during processing!
            # Worker must NOT falsely acknowledge successful completion!
            logger.error(
                f"Database unavailable during processing of report {report_id}: {exc}",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "action": "database_failure",
                    "error_category": "database_unavailable",
                },
                exc_info=True,
            )
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                except Exception:
                    pass

            # Nack with requeue=True so work is not lost and another worker / retry picks it up
            try:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                pass

        except Exception as exc:
            logger.error(
                f"Unexpected exception processing report {report_id}: {exc}",
                extra={
                    "report_id": report_id,
                    "worker_id": self.worker_id,
                    "action": "unexpected_error",
                    "error_category": "system_error",
                },
                exc_info=True,
            )
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                except Exception:
                    pass

            db.rollback()
            try:
                rep = db.query(Report).filter(Report.id == report_id).first()
                if rep:
                    rep.status = "failed"
                    rep.error_message = f"Unexpected processing error: {exc}"
                    rep.error_category = "system_error"
                    rep.updated_at = datetime.now(timezone.utc)
                    db.commit()

                publish_dlq_message(
                    report_id=str(report_id),
                    attempt=current_attempt,
                    error_message=str(exc),
                    error_category="system_error",
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as inner_err:
                logger.error(f"Failed to record error state in DB: {inner_err}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

        finally:
            db.close()


if __name__ == "__main__":
    worker_arg = sys.argv[1] if len(sys.argv) > 1 else None
    worker = ReportWorker(worker_id=worker_arg)
    worker.run()

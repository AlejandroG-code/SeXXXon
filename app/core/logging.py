import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class StructuredJsonFormatter(logging.Formatter):
    """
    Structured JSON log formatter meeting project requirements:
    - Report ID
    - Worker identifier
    - Attempt number
    - Timestamp (ISO 8601 UTC)
    - Action or status
    - Error category (when applicable)
    - Credentials are never logged.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        # Contextual attributes
        if hasattr(record, "report_id") and record.report_id:
            log_data["report_id"] = str(record.report_id)
        if hasattr(record, "worker_id") and record.worker_id:
            log_data["worker_id"] = str(record.worker_id)
        if hasattr(record, "attempt_number") and record.attempt_number is not None:
            log_data["attempt_number"] = record.attempt_number
        if hasattr(record, "action") and record.action:
            log_data["action"] = str(record.action)
        if hasattr(record, "status") and record.status:
            log_data["status"] = str(record.status)
        if hasattr(record, "error_category") and record.error_category:
            log_data["error_category"] = str(record.error_category)

        # Include exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logger(name: str = "app", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredJsonFormatter())
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = setup_logger("report_system")

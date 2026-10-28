import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/reports_db"

    # RabbitMQ
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_MANAGEMENT_URL: str = "http://localhost:15672/"

    # Queue Topology
    EXCHANGE_NAME: str = "reports.direct"
    QUEUE_NAME: str = "reports.queue"
    ROUTING_KEY: str = "reports.process"

    RETRY_EXCHANGE: str = "reports.retry.direct"
    RETRY_QUEUE: str = "reports.retry.queue"

    DLX_EXCHANGE: str = "reports.dlx"
    DLQ_QUEUE: str = "reports.dlq"
    DLQ_ROUTING_KEY: str = "reports.failed"

    # File Storage
    STORAGE_DIR: str = "./storage/reports"

    # Lifecycle & Retry
    WORKER_PROCESSING_DELAY_SECONDS: int = 5
    RETRY_DELAY_SECONDS: int = 5
    MAX_ATTEMPTS: int = 3

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    def get_storage_path(self) -> Path:
        path = Path(self.STORAGE_DIR).resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path


settings = Settings()

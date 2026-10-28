import json
import logging
from typing import Any, Optional
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError, UnroutableError, NackError
from app.core.config import settings
from app.core.logging import logger


def get_rabbitmq_connection() -> pika.BlockingConnection:
    """Create and return a blocking connection to RabbitMQ with heartbeat and fast timeout."""
    parameters = pika.URLParameters(settings.RABBITMQ_URL)
    parameters.heartbeat = 60
    parameters.blocked_connection_timeout = 10
    parameters.socket_timeout = 2.0
    parameters.connection_attempts = 1
    parameters.retry_delay = 1
    return pika.BlockingConnection(parameters)


def setup_rabbitmq_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """
    Declare exchanges, queues, and bindings for durable, reliable processing:
    1. reports.direct exchange -> reports.queue
    2. reports.retry.direct exchange -> reports.retry.queue (with TTL & DLX back to reports.direct)
    3. reports.dlx exchange -> reports.dlq (for inspection after max attempts/permanent failures)
    """
    # 1. Dead Letter Exchange & Queue for unrecoverable/exhausted messages
    channel.exchange_declare(
        exchange=settings.DLX_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=settings.DLQ_QUEUE,
        durable=True,
    )
    channel.queue_bind(
        queue=settings.DLQ_QUEUE,
        exchange=settings.DLX_EXCHANGE,
        routing_key=settings.DLQ_ROUTING_KEY,
    )

    # 2. Main Processing Exchange & Queue
    channel.exchange_declare(
        exchange=settings.EXCHANGE_NAME,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=settings.QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": settings.DLX_EXCHANGE,
            "x-dead-letter-routing-key": settings.DLQ_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=settings.QUEUE_NAME,
        exchange=settings.EXCHANGE_NAME,
        routing_key=settings.ROUTING_KEY,
    )

    # 3. Retry Exchange & Delayed Queue (TTL -> dead-letter back to main queue)
    channel.exchange_declare(
        exchange=settings.RETRY_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=settings.RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": settings.RETRY_DELAY_SECONDS * 1000,
            "x-dead-letter-exchange": settings.EXCHANGE_NAME,
            "x-dead-letter-routing-key": settings.ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=settings.RETRY_QUEUE,
        exchange=settings.RETRY_EXCHANGE,
        routing_key="reports.retry",
    )


def publish_job_message(
    report_id: str,
    attempt: int = 1,
    simulate_failure: Optional[str] = None,
) -> bool:
    """
    Publish a report processing job using Publisher Confirms and persistent messages.
    Returns True if confirmed by the broker, False otherwise.
    """
    payload = {
        "report_id": str(report_id),
        "attempt": attempt,
        "simulate_failure": simulate_failure,
    }
    body = json.dumps(payload)

    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        setup_rabbitmq_topology(channel)

        # Enable publisher confirms
        channel.confirm_delivery()

        properties = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers={"x-attempt": attempt},
        )

        # Publish with mandatory=True to verify routability
        channel.basic_publish(
            exchange=settings.EXCHANGE_NAME,
            routing_key=settings.ROUTING_KEY,
            body=body,
            properties=properties,
            mandatory=True,
        )

        logger.info(
            f"Published report job {report_id} (attempt {attempt}) to {settings.QUEUE_NAME}",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "publish_job",
                "status": "queued",
            },
        )
        return True

    except Exception as exc:
        logger.error(
            f"Failed to publish report job {report_id}: {exc}",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "publish_failed",
                "status": "pending",
                "error_category": "broker_error",
            },
            exc_info=True,
        )
        return False
    finally:
        if connection and connection.is_open:
            connection.close()


def publish_retry_message(
    report_id: str,
    attempt: int,
    simulate_failure: Optional[str] = None,
) -> bool:
    """Publish a job to the retry queue for delayed reprocessing."""
    payload = {
        "report_id": str(report_id),
        "attempt": attempt,
        "simulate_failure": simulate_failure,
    }
    body = json.dumps(payload)

    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        setup_rabbitmq_topology(channel)
        channel.confirm_delivery()

        properties = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers={"x-attempt": attempt},
        )

        channel.basic_publish(
            exchange=settings.RETRY_EXCHANGE,
            routing_key="reports.retry",
            body=body,
            properties=properties,
            mandatory=True,
        )

        logger.info(
            f"Published report {report_id} to retry queue with delay {settings.RETRY_DELAY_SECONDS}s",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "publish_retry",
                "status": "retrying",
            },
        )
        return True
    except Exception as exc:
        logger.error(
            f"Failed to publish retry for report {report_id}: {exc}",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "retry_publish_failed",
                "error_category": "broker_error",
            },
            exc_info=True,
        )
        return False
    finally:
        if connection and connection.is_open:
            connection.close()


def publish_dlq_message(
    report_id: str,
    attempt: int,
    error_message: str,
    error_category: str = "permanent",
) -> bool:
    """Publish a permanently failed job to the Dead Letter Queue for inspection."""
    payload = {
        "report_id": str(report_id),
        "attempt": attempt,
        "error_message": error_message,
        "error_category": error_category,
    }
    body = json.dumps(payload)

    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        setup_rabbitmq_topology(channel)
        channel.confirm_delivery()

        properties = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers={"x-error-category": error_category},
        )

        channel.basic_publish(
            exchange=settings.DLX_EXCHANGE,
            routing_key=settings.DLQ_ROUTING_KEY,
            body=body,
            properties=properties,
            mandatory=True,
        )

        logger.warning(
            f"Published failed report {report_id} to DLQ {settings.DLQ_QUEUE}",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "publish_dlq",
                "status": "failed",
                "error_category": error_category,
            },
        )
        return True
    except Exception as exc:
        logger.error(
            f"Failed to publish to DLQ for report {report_id}: {exc}",
            extra={
                "report_id": report_id,
                "attempt_number": attempt,
                "action": "dlq_publish_failed",
                "error_category": "broker_error",
            },
            exc_info=True,
        )
        return False
    finally:
        if connection and connection.is_open:
            connection.close()

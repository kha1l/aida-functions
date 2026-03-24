"""Dagster-ресурс для публикации сообщений в RabbitMQ."""

from typing import Any, Dict

from dagster import BoolSource, Field, InitResourceContext, StringSource, resource

from config import rabbitmq_settings
from messaging import RabbitMQPublisher


@resource(
    config_schema={
        "url": Field(StringSource, is_required=False),
        "queue": Field(StringSource, is_required=False),
        "max_queue": Field(StringSource, is_required=False),
        "exchange": Field(StringSource, is_required=False),
        "routing_key": Field(StringSource, is_required=False),
        "exchange_type": Field(StringSource, is_required=False),
        "durable": Field(BoolSource, is_required=False),
        "enabled": Field(BoolSource, is_required=False),
    }
)
def rabbitmq_resource(context: InitResourceContext) -> RabbitMQPublisher:
    """Dagster-ресурс RabbitMQ с override параметрами из config_schema."""
    cfg = rabbitmq_settings()
    overrides: Dict[str, Any] = context.resource_config or {}

    return RabbitMQPublisher(
        url=overrides.get("url", cfg.url),
        queue=overrides.get("queue", cfg.queue),
        max_queue=overrides.get("max_queue", cfg.max_queue),
        exchange=overrides.get("exchange", cfg.exchange),
        routing_key=overrides.get("routing_key", cfg.routing_key),
        exchange_type=overrides.get("exchange_type", cfg.exchange_type),
        durable=overrides.get("durable", cfg.durable),
        enabled=overrides.get("enabled", cfg.enabled),
    )

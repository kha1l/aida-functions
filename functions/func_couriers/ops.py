"""Dagster ops для daily и manual-отчетов по курьерам."""
import asyncio

from dagster import Field, OpExecutionContext, op

from functions.shared.daily_job_config import DAILY_REPORT_CONFIG_SCHEMA

from .utils.op_certificates import run_certificates
from .utils.op_couriers import run_couriers
from .utils.op_later import run_later
from .utils.op_longers import run_longers
from .utils.op_sectors import run_sectors
from .utils.op_waiting import run_waiting


MANUAL_UNIT_REPORT_CONFIG = {
    "unit_uuid": Field(str),
    "account_id": Field(int),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
    "message_date": Field(str, is_required=False),
    "result_request_id": Field(str, is_required=False),
}

MANUAL_WAITING_REPORT_CONFIG = {
    **MANUAL_UNIT_REPORT_CONFIG,
    "wait_threshold_seconds": Field(int),
}


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=DAILY_REPORT_CONFIG_SCHEMA,
)
def couriers_op(context: OpExecutionContext):
    """Собирает отчет по курьерам и отправляет в RabbitMQ."""
    asyncio.run(run_couriers(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_UNIT_REPORT_CONFIG,
)
def certificates_op(context: OpExecutionContext):
    """Собирает отчет по сертификатам и отправляет в RabbitMQ."""
    asyncio.run(run_certificates(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_UNIT_REPORT_CONFIG,
)
def later_op(context: OpExecutionContext):
    """Собирает отчет по опозданиям и прогулам и отправляет в RabbitMQ."""
    asyncio.run(run_later(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_WAITING_REPORT_CONFIG,
)
def waiting_op(context: OpExecutionContext):
    """Собирает отчет по ожиданиям курьеров для RabbitMQ или WebApp."""
    asyncio.run(run_waiting(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_UNIT_REPORT_CONFIG,
)
def sectors_op(context: OpExecutionContext):
    """Собирает отчет по секторам доставки и отправляет в RabbitMQ."""
    asyncio.run(run_sectors(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_UNIT_REPORT_CONFIG,
)
def longers_op(context: OpExecutionContext):
    """Собирает подробный отчет по долгим доставкам и отправляет в RabbitMQ."""
    asyncio.run(run_longers(context))

"""Dagster ops для daily и manual-отчетов по ресторану."""
import asyncio

from dagster import Field, OpExecutionContext, op

from .utils.op_drink_rest import run_drink_rest
from .utils.op_assembly_rest import run_assembly_rest
from .utils.op_later_rest import run_later_rest
from .utils.op_stationary import run_stationary


MANUAL_STATIONARY_REPORT_CONFIG = {
    "unit_uuid": Field(str),
    "account_id": Field(int),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
    "message_date": Field(str, is_required=False),
    "result_request_id": Field(str, is_required=False),
}

MANUAL_STATIONARY_DRINK_REPORT_CONFIG = {
    **MANUAL_STATIONARY_REPORT_CONFIG,
    "drink_threshold_seconds": Field(int),
}


@op(required_resource_keys={"db", "rabbitmq", "dodo_api"})
def stationary_op(context: OpExecutionContext):
    """Собирает отчет по ресторану и отправляет его в RabbitMQ."""
    asyncio.run(run_stationary(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_STATIONARY_REPORT_CONFIG,
)
def later_rest_op(context: OpExecutionContext):
    """Собирает подробный отчет по долгим заказам ресторана и отправляет его в RabbitMQ."""
    asyncio.run(run_later_rest(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_STATIONARY_REPORT_CONFIG,
)
def assembly_rest_op(context: OpExecutionContext):
    """Собирает подробный отчет по долгой сборке заказов и отправляет его в RabbitMQ."""
    asyncio.run(run_assembly_rest(context))


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=MANUAL_STATIONARY_DRINK_REPORT_CONFIG,
)
def drink_rest_op(context: OpExecutionContext):
    """Собирает подробный отчет по долгим напиткам и отправляет его в RabbitMQ."""
    asyncio.run(run_drink_rest(context))

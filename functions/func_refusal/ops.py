import asyncio

from dagster import OpExecutionContext, op
from security_logging import safe_log_context

from .utils.op_refusal import run_refusal


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def refusal_op(context: OpExecutionContext):
    """Запускает hourly-отчет по отмененным заказам."""
    asyncio.run(run_refusal(safe_log_context(context)))

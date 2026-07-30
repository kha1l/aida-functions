import asyncio

from dagster import OpExecutionContext, op

from functions.shared.daily_job_config import DAILY_REPORT_CONFIG_SCHEMA
from security_logging import safe_log_context

from .utils.op_birthday import run_birthday


@op(
    required_resource_keys={"db", "dodo_api", "zveno", "rabbitmq"},
    config_schema=DAILY_REPORT_CONFIG_SCHEMA,
)
def birthday_op(context: OpExecutionContext):
    """Запускает ежедневную отправку отчетов по дням рождения."""
    asyncio.run(run_birthday(safe_log_context(context)))

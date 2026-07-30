"""Dagster op для регулярного отчета по клиентским тикетам."""

import asyncio

from dagster import OpExecutionContext, op
from security_logging import safe_log_context

from .utils.op_tickets import run_tickets


@op(required_resource_keys={"db", "rabbitmq", "pyrus", "dodo_api"})
def tickets_op(context: OpExecutionContext) -> None:
    """Запускает сбор тикетов из Pyrus за последние 5 минут."""
    asyncio.run(run_tickets(safe_log_context(context)))

"""Dagster op для регулярного отчета по складским остаткам."""

from dagster import Field, OpExecutionContext, op
from security_logging import safe_log_context

from .utils.op_stock import run_stock, run_stock_manual


MANUAL_STOCK_CONFIG = {
    "unit_uuids": Field([str]),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
}


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def stock_op(context: OpExecutionContext) -> None:
    """Запускает асинхронную сборку и отправку stock-отчетов."""
    import asyncio

    asyncio.run(run_stock(safe_log_context(context)))


@op(
    required_resource_keys={"db", "dodo_api", "rabbitmq"},
    config_schema=MANUAL_STOCK_CONFIG,
)
def stock_manual_op(context: OpExecutionContext) -> None:
    """Запускает manual-сборку складских остатков по выбранным заведениям."""
    import asyncio

    asyncio.run(run_stock_manual(safe_log_context(context)))

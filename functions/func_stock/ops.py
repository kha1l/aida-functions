"""Dagster op для регулярного отчета по складским остаткам."""

from dagster import OpExecutionContext, op

from .utils.op_stock import run_stock


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def stock_op(context: OpExecutionContext) -> None:
    """Запускает асинхронную сборку и отправку stock-отчетов."""
    import asyncio

    asyncio.run(run_stock(context))

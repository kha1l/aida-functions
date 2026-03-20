"""Dagster op для регулярного отчета по стопам."""

import asyncio

from dagster import OpExecutionContext, op

from .utils.op_stops import run_stops


@op(required_resource_keys={"db", "rabbitmq", "dodo_api"})
def stops_op(context: OpExecutionContext) -> None:
    """Запускает сбор всех типов stop-отчетов за предыдущее 5-минутное окно."""
    asyncio.run(run_stops(context))

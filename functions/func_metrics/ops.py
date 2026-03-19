"""Dagster op для регулярного отчета по ключевым метрикам заведений."""

from dagster import OpExecutionContext, op

from .utils.op_metrics import run_metrics


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def metrics_op(context: OpExecutionContext) -> None:
    """Запускает асинхронную сборку и отправку метрик по подпискам."""
    import asyncio

    asyncio.run(run_metrics(context))

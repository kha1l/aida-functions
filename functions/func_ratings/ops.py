"""Dagster-op для ежедневного отчета по рейтингам."""

from dagster import OpExecutionContext, op

from .utils.op_ratings import run_ratings


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def ratings_op(context: OpExecutionContext) -> None:
    """Запускает асинхронную сборку и отправку отчета по рейтингам."""
    import asyncio

    asyncio.run(run_ratings(context))

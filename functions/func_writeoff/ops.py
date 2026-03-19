import asyncio

from dagster import OpExecutionContext, op

from .utils.op_writeoff import run_writeoff


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def writeoff_op(context: OpExecutionContext):
    """Собирает отчет по списаниям и отправляет его в RabbitMQ."""
    asyncio.run(run_writeoff(context))

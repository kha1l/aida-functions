import asyncio

from dagster import OpExecutionContext, op

from .utils.op_defect import run_defect


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def defect_op(context: OpExecutionContext):
    """Запускает hourly-отчет по браку."""
    asyncio.run(run_defect(context))

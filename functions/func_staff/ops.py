import asyncio

from dagster import OpExecutionContext, op

from .utils.op_staff import run_staff


@op(required_resource_keys={"db", "dodo_api", "openrouter", "rabbitmq"})
def staff_op(context: OpExecutionContext):
    """Запускает ежедневную отправку отчетов по юбилеям и годовщинам работы."""
    asyncio.run(run_staff(context))

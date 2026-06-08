import asyncio

from dagster import OpExecutionContext, op

from .utils.op_birthday import run_birthday


@op(required_resource_keys={"db", "dodo_api", "zveno", "rabbitmq"})
def birthday_op(context: OpExecutionContext):
    """Запускает ежедневную отправку отчетов по дням рождения."""
    asyncio.run(run_birthday(context))

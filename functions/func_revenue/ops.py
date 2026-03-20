"""Dagster-op для отчета по выручке сети."""

from dagster import Field, OpExecutionContext, op

from .utils.op_revenue import run_revenue, run_revenue_manual


MANUAL_REVENUE_CONFIG = {
    "unit_uuids": Field([str]),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
}


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
async def revenue_op(context: OpExecutionContext):
    """Запускает сбор и отправку отчета по выручке сети."""
    await run_revenue(context)


@op(
    required_resource_keys={"db", "dodo_api", "rabbitmq"},
    config_schema=MANUAL_REVENUE_CONFIG,
)
async def revenue_manual_op(context: OpExecutionContext):
    """Запускает manual-сборку выручки по выбранным заведениям."""
    await run_revenue_manual(context)

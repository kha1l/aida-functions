"""Dagster op для регулярного отчета по ключевым метрикам заведений."""

from dagster import OpExecutionContext, op
from dagster import Field
from security_logging import safe_log_context

from .utils.op_metrics import run_metrics
from .utils.op_metrics import run_metrics_manual


MANUAL_METRICS_CONFIG = {
    "unit_uuids": Field([str]),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
}


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
def metrics_op(context: OpExecutionContext) -> None:
    """Запускает асинхронную сборку и отправку метрик по подпискам."""
    import asyncio

    asyncio.run(run_metrics(safe_log_context(context)))


@op(
    required_resource_keys={"db", "dodo_api", "rabbitmq"},
    config_schema=MANUAL_METRICS_CONFIG,
)
def metrics_manual_op(context: OpExecutionContext) -> None:
    """Запускает manual-сборку метрик по выбранным заведениям."""
    import asyncio

    asyncio.run(run_metrics_manual(safe_log_context(context)))

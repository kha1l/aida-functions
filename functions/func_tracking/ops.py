"""Dagster op для manual-отчета по трекингу."""

import asyncio

from dagster import Field, OpExecutionContext, op

from .utils.op_tracking import run_tracking


TRACKING_OP_CONFIG = {
    "unit_uuids": Field([str]),
    "chat_id": Field(int),
    "topic_id": Field(int, is_required=False),
}


@op(
    required_resource_keys={"db", "rabbitmq", "dodo_api"},
    config_schema=TRACKING_OP_CONFIG,
)
def tracking_op(context: OpExecutionContext) -> None:
    """Запускает manual-отчет по трекингу для выбранных заведений."""
    asyncio.run(run_tracking(context))

"""Dagster ops для регулярного обновления store widget."""

import asyncio

from dagster import OpExecutionContext, op

from .utils.op_widget import run_widget


@op(required_resource_keys={"db", "dodo_api"})
def widget_op(context: OpExecutionContext):
    """Собирает сетевую выручку и выгружает ее в store-api/storage."""
    asyncio.run(run_widget(context))

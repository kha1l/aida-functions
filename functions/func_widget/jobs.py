from dagster import job

from .ops import widget_op


@job
def widget_job():
    """Запускает Dagster job для обновления store widget."""
    widget_op()

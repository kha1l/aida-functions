from dagster import job

from .ops import metrics_op


@job
def metrics_job():
    """Запускает Dagster job для отчёта metrics."""
    metrics_op()

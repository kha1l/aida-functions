from dagster import job

from .ops import metrics_manual_op, metrics_op


@job
def metrics_job():
    """Запускает Dagster job для отчёта metrics."""
    metrics_op()


@job
def metrics_manual_job():
    """Запускает manual job для оперативного отчета по метрикам."""
    metrics_manual_op()

from dagster import job

from .ops import revenue_manual_op, revenue_op


@job
def revenue_job():
    """Запускает Dagster job для отчёта revenue."""
    revenue_op()


@job
def revenue_manual_job():
    """Запускает manual job для оперативного отчета по выручке."""
    revenue_manual_op()

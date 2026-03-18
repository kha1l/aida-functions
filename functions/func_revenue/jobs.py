from dagster import job

from .ops import revenue_op


@job
def revenue_job():
    """Запускает Dagster job для отчёта revenue."""
    revenue_op()

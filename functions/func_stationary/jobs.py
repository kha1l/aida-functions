from dagster import job

from .ops import stationary_op


@job
def stationary_job():
    """Запускает Dagster job для отчёта stationary."""
    stationary_op()

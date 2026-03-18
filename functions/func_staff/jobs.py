from dagster import job

from .ops import staff_op


@job
def staff_job():
    """Запускает Dagster job для отчёта staff."""
    staff_op()

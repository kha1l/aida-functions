from dagster import job

from .ops import tickets_op


@job
def tickets_job():
    """Запускает Dagster job для отчёта tickets."""
    tickets_op()

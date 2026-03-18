from dagster import job

from .ops import couriers_op


@job
def couriers_job():
    """Запускает Dagster job для отчёта couriers."""
    couriers_op()

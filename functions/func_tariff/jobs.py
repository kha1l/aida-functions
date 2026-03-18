from dagster import job

from .ops import tariff_op


@job
def tariff_job():
    """Запускает Dagster job для отчёта tariff."""
    tariff_op()

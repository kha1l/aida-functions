from dagster import job

from .ops import stock_op


@job
def stock_job():
    """Запускает Dagster job для отчёта stock."""
    stock_op()

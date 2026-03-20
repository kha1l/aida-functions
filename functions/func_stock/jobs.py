from dagster import job

from .ops import stock_manual_op, stock_op


@job
def stock_job():
    """Запускает Dagster job для отчёта stock."""
    stock_op()


@job
def stock_manual_job():
    """Запускает manual job для оперативного отчета по складу."""
    stock_manual_op()

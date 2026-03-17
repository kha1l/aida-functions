from dagster import job

from .ops import stock_op


@job
def stock_job():
    stock_op()

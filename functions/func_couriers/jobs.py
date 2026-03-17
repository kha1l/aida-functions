from dagster import job

from .ops import couriers_op


@job
def couriers_job():
    couriers_op()

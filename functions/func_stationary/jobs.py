from dagster import job

from .ops import stationary_op


@job
def stationary_job():
    stationary_op()

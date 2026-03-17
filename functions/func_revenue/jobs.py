from dagster import job

from .ops import revenue_op


@job
def revenue_job():
    revenue_op()

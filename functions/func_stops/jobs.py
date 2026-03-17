from dagster import job

from .ops import stops_op


@job
def stops_job():
    stops_op()

from dagster import job

from .ops import staff_op


@job
def staff_job():
    staff_op()

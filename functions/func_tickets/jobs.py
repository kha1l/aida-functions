from dagster import job

from .ops import tickets_op


@job
def tickets_job():
    tickets_op()

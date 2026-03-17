from dagster import job

from .ops import birthday_op


@job
def birthday_job():
    birthday_op()

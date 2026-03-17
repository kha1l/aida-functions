from dagster import job

from .ops import refusal_op


@job
def refusal_job():
    refusal_op()

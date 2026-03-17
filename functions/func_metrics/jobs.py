from dagster import job

from .ops import metrics_op


@job
def metrics_job():
    metrics_op()

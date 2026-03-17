from dagster import job

from .ops import tariff_op


@job
def tariff_job():
    tariff_op()

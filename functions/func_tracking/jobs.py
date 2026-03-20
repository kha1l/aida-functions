"""Dagster job для manual-отчета по трекингу."""

from dagster import job

from .ops import tracking_op


@job
def tracking_job():
    """Запускает manual job по сводному трекингу."""
    tracking_op()

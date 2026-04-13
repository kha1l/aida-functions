"""Dagster job для manual-отчета по трекингу."""

from dagster import job

from .ops import tracking_op


@job(description="Ручной сводный отчет по трекингу выбранных заведений.")
def tracking_job():
    """Запускает manual job по сводному трекингу."""
    tracking_op()

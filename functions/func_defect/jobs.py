from dagster import job

from .ops import defect_op


@job
def defect_job():
    """Запускает Dagster job для hourly-отчета по браку."""
    defect_op()

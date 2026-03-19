from dagster import job

from .ops import writeoff_op


@job
def writeoff_job():
    """Запускает Dagster job для отчета по списаниям."""
    writeoff_op()

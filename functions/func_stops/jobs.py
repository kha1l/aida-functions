from dagster import job

from .ops import stops_op


@job
def stops_job():
    """Запускает Dagster job для всех типов stop-отчетов."""
    stops_op()

from dagster import job

from .ops import ratings_op


@job
def ratings_job():
    """Запускает Dagster job для отчета ratings."""
    ratings_op()

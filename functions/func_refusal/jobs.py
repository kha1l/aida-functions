from dagster import job

from .ops import refusal_op


@job
def refusal_job():
    """Запускает Dagster job для отчёта refusal."""
    refusal_op()

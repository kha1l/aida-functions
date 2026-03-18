from dagster import job

from .ops import birthday_op


@job
def birthday_job():
    """Запускает Dagster job для отчёта birthdays."""
    birthday_op()

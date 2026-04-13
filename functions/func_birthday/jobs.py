from dagster import job

from .ops import birthday_op


@job(description="Ежедневный отчет по дням рождения сотрудников для подписанных заведений.")
def birthday_job():
    """Запускает Dagster job для отчёта birthdays."""
    birthday_op()

from dagster import job

from .ops import tickets_op


@job(description="Регулярная обработка и отправка отчетов по тикетам и обращениям.")
def tickets_job():
    """Запускает Dagster job для отчёта tickets."""
    tickets_op()

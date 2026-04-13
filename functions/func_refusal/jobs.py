from dagster import job

from .ops import refusal_op


@job(description="Регулярный отчет по отмененным заказам с причинами, составом и суммами.")
def refusal_job():
    """Запускает Dagster job для отчёта refusal."""
    refusal_op()

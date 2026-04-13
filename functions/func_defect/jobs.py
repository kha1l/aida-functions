from dagster import job

from .ops import defect_op


@job(description="Почасовой отчет по браку: количество, стоимость и детализация проблемных позиций.")
def defect_job():
    """Запускает Dagster job для hourly-отчета по браку."""
    defect_op()

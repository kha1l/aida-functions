from dagster import job

from .ops import writeoff_op


@job(description="Регулярный отчет по списаниям: суммы, причины, частые и дорогие позиции.")
def writeoff_job():
    """Запускает Dagster job для отчета по списаниям."""
    writeoff_op()

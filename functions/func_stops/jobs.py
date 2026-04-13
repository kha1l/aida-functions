from dagster import job

from .ops import stops_op


@job(description="Регулярный отчет по стоп-продажам во всех поддерживаемых разрезах.")
def stops_job():
    """Запускает Dagster job для всех типов stop-отчетов."""
    stops_op()

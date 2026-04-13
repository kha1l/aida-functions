from dagster import job

from .ops import widget_op


@job(description="Регулярное обновление данных store widget.")
def widget_job():
    """Запускает Dagster job для обновления store widget."""
    widget_op()

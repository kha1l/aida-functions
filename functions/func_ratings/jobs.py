from dagster import job

from .ops import ratings_op


@job(description="Регулярный отчет по рейтингам и оценкам заказов.")
def ratings_job():
    """Запускает Dagster job для отчета ratings."""
    ratings_op()

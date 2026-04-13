from dagster import job

from .ops import metrics_manual_op, metrics_op


@job(description="Регулярный отчет по ключевым метрикам заведения: производительность, скорость, оценки и сертификаты.")
def metrics_job():
    """Запускает Dagster job для отчёта metrics."""
    metrics_op()


@job(description="Ручной оперативный запуск отчета по ключевым метрикам выбранных заведений.")
def metrics_manual_job():
    """Запускает manual job для оперативного отчета по метрикам."""
    metrics_manual_op()

from dagster import job

from .ops import revenue_manual_op, revenue_op


@job(description="Регулярный отчет по выручке сети и заведений.")
def revenue_job():
    """Запускает Dagster job для отчёта revenue."""
    revenue_op()


@job(description="Ручной оперативный запуск отчета по выручке за выбранный период.")
def revenue_manual_job():
    """Запускает manual job для оперативного отчета по выручке."""
    revenue_manual_op()

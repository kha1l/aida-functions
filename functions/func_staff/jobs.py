from dagster import job

from .ops import staff_op


@job(description="Регулярный отчет по сотрудникам: статусы, смены и связанные кадровые показатели.")
def staff_job():
    """Запускает Dagster job для отчёта staff."""
    staff_op()

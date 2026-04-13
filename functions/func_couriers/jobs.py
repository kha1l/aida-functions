from dagster import job

from .ops import certificates_op, couriers_op, later_op, longers_op, sectors_op, waiting_op


@job(description="Краткий регулярный отчет по доставке: выручка, заказы, скорость, полка, опоздания и загрузка курьеров.")
def couriers_job():
    """Запускает Dagster job для отчёта couriers."""
    couriers_op()


@job(description="Отчет по сертификатам курьеров: контроль актуальности и проблемных сертификатов.")
def certificates_job():
    """Запускает Dagster job для отчёта certificates."""
    certificates_op()


@job(description="Отчет по опозданиям и прогулам сотрудников доставки.")
def later_job():
    """Запускает Dagster job для отчёта по опозданиям и прогулам."""
    later_op()


@job(description="Отчет по ожиданиям курьеров и простоям на доставке.")
def waiting_job():
    """Запускает Dagster job для отчёта по ожиданиям курьеров."""
    waiting_op()


@job(description="Отчет по секторам доставки и их текущему состоянию.")
def sectors_job():
    """Запускает Dagster job для отчёта по секторам доставки."""
    sectors_op()


@job(description="Подробный отчет по долгим доставкам и проблемным заказам.")
def longers_job():
    """Запускает Dagster job для подробного отчёта по долгим доставкам."""
    longers_op()

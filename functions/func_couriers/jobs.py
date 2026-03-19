from dagster import job

from .ops import certificates_op, couriers_op, later_op, longers_op, sectors_op, waiting_op


@job
def couriers_job():
    """Запускает Dagster job для отчёта couriers."""
    couriers_op()


@job
def certificates_job():
    """Запускает Dagster job для отчёта certificates."""
    certificates_op()


@job
def later_job():
    """Запускает Dagster job для отчёта по опозданиям и прогулам."""
    later_op()


@job
def waiting_job():
    """Запускает Dagster job для отчёта по ожиданиям курьеров."""
    waiting_op()


@job
def sectors_job():
    """Запускает Dagster job для отчёта по секторам доставки."""
    sectors_op()


@job
def longers_job():
    """Запускает Dagster job для подробного отчёта по долгим доставкам."""
    longers_op()

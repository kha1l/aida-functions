from dagster import job

from .ops import assembly_rest_op, drink_rest_op, later_rest_op, stationary_op


@job
def stationary_job():
    """Запускает Dagster job для отчёта stationary."""
    stationary_op()


@job
def later_rest_job():
    """Запускает manual job для долгих заказов ресторана."""
    later_rest_op()


@job
def assembly_rest_job():
    """Запускает manual job для долгой сборки заказов ресторана."""
    assembly_rest_op()


@job
def drink_rest_job():
    """Запускает manual job для отчета по долгим напиткам."""
    drink_rest_op()

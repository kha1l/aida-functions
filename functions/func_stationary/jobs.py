from dagster import job

from .ops import assembly_rest_op, drink_rest_op, later_rest_op, stationary_op


@job(description="Краткий регулярный отчет по ресторану: выручка, заказы, скорость приготовления и продажи через каналы.")
def stationary_job():
    """Запускает Dagster job для отчёта stationary."""
    stationary_op()


@job(description="Ручной отчет по долгим ресторанным заказам.")
def later_rest_job():
    """Запускает manual job для долгих заказов ресторана."""
    later_rest_op()


@job(description="Ручной отчет по долгой сборке заказов ресторана.")
def assembly_rest_job():
    """Запускает manual job для долгой сборки заказов ресторана."""
    assembly_rest_op()


@job(description="Ручной отчет по долгому приготовлению напитков.")
def drink_rest_job():
    """Запускает manual job для отчета по долгим напиткам."""
    drink_rest_op()

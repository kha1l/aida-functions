from dagster import job

from .ops import stock_manual_op, stock_op


@job(description="Регулярный отчет по критичным складским остаткам: некорректные остатки и позиции, которых хватит меньше чем на 1-2 дня.")
def stock_job():
    """Запускает Dagster job для отчёта stock."""
    stock_op()


@job(description="Ручной оперативный запуск отчета по складским остаткам выбранных заведений.")
def stock_manual_job():
    """Запускает manual job для оперативного отчета по складу."""
    stock_manual_op()

"""Корневой Dagster Definitions для всех jobs, schedules и shared-ресурсов."""

from dagster import Definitions

from functions.func_birthday.jobs import birthday_job
from functions.func_birthday.schedulers import birthday_schedule
from functions.func_couriers.jobs import certificates_job, couriers_job, later_job, longers_job, sectors_job, waiting_job
from functions.func_couriers.schedulers import couriers_schedule
from functions.func_defect.jobs import defect_job
from functions.func_defect.schedulers import defect_schedule
from functions.func_metrics.jobs import metrics_job
from functions.func_metrics.schedulers import metrics_schedule
from functions.func_refusal.jobs import refusal_job
from functions.func_refusal.schedulers import refusal_schedule
from functions.func_revenue.jobs import revenue_job
from functions.func_revenue.schedulers import revenue_schedule
from functions.func_staff.jobs import staff_job
from functions.func_staff.schedulers import staff_schedule
from functions.func_stationary.jobs import assembly_rest_job, drink_rest_job, later_rest_job, stationary_job
from functions.func_stationary.schedulers import stationary_schedule
from functions.func_stock.jobs import stock_job
from functions.func_stock.schedulers import stock_schedule
from functions.func_stops.jobs import stops_job
from functions.func_stops.schedulers import stops_schedule
from functions.func_tickets.jobs import tickets_job
from functions.func_tickets.schedulers import tickets_schedule
from functions.func_writeoff.jobs import writeoff_job
from functions.func_writeoff.schedulers import writeoff_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


# Все job'ы регистрируем централизованно, чтобы `dagster dev -m defs` видел их сразу.
_jobs = [
    birthday_job,
    certificates_job,
    couriers_job,
    later_job,
    longers_job,
    sectors_job,
    waiting_job,
    defect_job,
    metrics_job,
    refusal_job,
    revenue_job,
    staff_job,
    assembly_rest_job,
    drink_rest_job,
    later_rest_job,
    stationary_job,
    stock_job,
    stops_job,
    tickets_job,
    writeoff_job,
]

# Manual-job'ы без расписания сюда не добавляем специально.
_schedules = [
    birthday_schedule,
    couriers_schedule,
    defect_schedule,
    metrics_schedule,
    refusal_schedule,
    revenue_schedule,
    staff_schedule,
    stationary_schedule,
    stock_schedule,
    stops_schedule,
    tickets_schedule,
    writeoff_schedule,
]


# Общие ресурсы доступны во всех функциях через required_resource_keys.
resource_defs = COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=_jobs,
    schedules=[schedule for schedule in _schedules if schedule is not None],
    resources=resource_defs,
)

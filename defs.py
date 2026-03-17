from dagster import Definitions

from resources import db_resource, rabbitmq_resource

from functions.func_birthday.jobs import birthday_job
from functions.func_birthday.schedulers import birthday_schedule
from functions.func_couriers.jobs import couriers_job
from functions.func_couriers.schedulers import couriers_schedule
from functions.func_metrics.jobs import metrics_job
from functions.func_metrics.schedulers import metrics_schedule
from functions.func_refusal.jobs import refusal_job
from functions.func_refusal.schedulers import refusal_schedule
from functions.func_revenue.jobs import revenue_job
from functions.func_revenue.schedulers import revenue_schedule
from functions.func_staff.jobs import staff_job
from functions.func_staff.schedulers import staff_schedule
from functions.func_stationary.jobs import stationary_job
from functions.func_stationary.schedulers import stationary_schedule
from functions.func_stock.jobs import stock_job
from functions.func_stock.schedulers import stock_schedule
from functions.func_stops.jobs import stops_job
from functions.func_stops.schedulers import stops_schedule
from functions.func_tariff.jobs import tariff_job
from functions.func_tariff.schedulers import tariff_schedule
from functions.func_tickets.jobs import tickets_job
from functions.func_tickets.schedulers import tickets_schedule


_jobs = [
    birthday_job,
    couriers_job,
    metrics_job,
    refusal_job,
    revenue_job,
    staff_job,
    stationary_job,
    stock_job,
    stops_job,
    tariff_job,
    tickets_job,
]

_schedules = [
    birthday_schedule,
    couriers_schedule,
    metrics_schedule,
    refusal_schedule,
    revenue_schedule,
    staff_schedule,
    stationary_schedule,
    stock_schedule,
    stops_schedule,
    tariff_schedule,
    tickets_schedule,
]


resource_defs = {
    "db": db_resource,
    "rabbitmq": rabbitmq_resource,
}


defs = Definitions(
    jobs=_jobs,
    schedules=[schedule for schedule in _schedules if schedule is not None],
    resources=resource_defs,
)

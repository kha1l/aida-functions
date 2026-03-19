"""Модульные Definitions для delivery-отчетов и manual jobs по курьерам."""

from dagster import Definitions

from .jobs import certificates_job, couriers_job, later_job, longers_job, sectors_job, waiting_job
from .schedulers import couriers_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[couriers_job, certificates_job, later_job, waiting_job, sectors_job, longers_job],
    schedules=[schedule for schedule in [couriers_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

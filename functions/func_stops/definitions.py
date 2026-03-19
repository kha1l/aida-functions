"""Модульные Definitions для job по стопам."""
from dagster import Definitions

from .jobs import stops_job
from .schedulers import stops_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[stops_job],
    schedules=[schedule for schedule in [stops_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

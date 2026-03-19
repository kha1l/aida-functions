"""Модульные Definitions для hourly job по отказам."""

from dagster import Definitions

from .jobs import refusal_job
from .schedulers import refusal_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[refusal_job],
    schedules=[schedule for schedule in [refusal_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

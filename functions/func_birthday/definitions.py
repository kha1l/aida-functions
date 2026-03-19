"""Модульные Definitions для daily job по дням рождения."""

from dagster import Definitions

from .jobs import birthday_job
from .schedulers import birthday_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[birthday_job],
    schedules=[schedule for schedule in [birthday_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

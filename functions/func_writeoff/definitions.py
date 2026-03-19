"""Модульные Definitions для daily job по списаниям."""

from dagster import Definitions

from .jobs import writeoff_job
from .schedulers import writeoff_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[writeoff_job],
    schedules=[schedule for schedule in [writeoff_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

"""Модульные Definitions для daily job по юбилеям и стажу."""

from dagster import Definitions

from .jobs import staff_job
from .schedulers import staff_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[staff_job],
    schedules=[schedule for schedule in [staff_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

"""Модульные Definitions для hourly job по браку."""

from dagster import Definitions

from .jobs import defect_job
from .schedulers import defect_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[defect_job],
    schedules=[schedule for schedule in [defect_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

"""Модульные Definitions для job по выручке сети."""
from dagster import Definitions

from .jobs import revenue_job, revenue_manual_job
from .schedulers import revenue_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[revenue_job, revenue_manual_job],
    schedules=[schedule for schedule in [revenue_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

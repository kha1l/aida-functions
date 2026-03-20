"""Модульные Definitions для job по ключевым метрикам."""
from dagster import Definitions

from .jobs import metrics_job, metrics_manual_job
from .schedulers import metrics_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[metrics_job, metrics_manual_job],
    schedules=[schedule for schedule in [metrics_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

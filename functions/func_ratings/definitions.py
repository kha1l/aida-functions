"""Модульные Definitions для job по рейтингам."""

from dagster import Definitions

from functions.shared.definition_resources import COMMON_RESOURCE_DEFS

from .jobs import ratings_job
from .schedulers import ratings_schedule


defs = Definitions(
    jobs=[ratings_job],
    schedules=[schedule for schedule in [ratings_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

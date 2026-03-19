from dagster import Definitions

from .jobs import tickets_job
from .schedulers import tickets_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[tickets_job],
    schedules=[schedule for schedule in [tickets_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

from dagster import Definitions

from .jobs import tickets_job
from .schedulers import tickets_schedule


defs = Definitions(
    jobs=[tickets_job],
    schedules=[schedule for schedule in [tickets_schedule] if schedule is not None],
)

definitions = defs

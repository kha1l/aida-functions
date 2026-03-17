from dagster import Definitions

from .jobs import stationary_job
from .schedulers import stationary_schedule


defs = Definitions(
    jobs=[stationary_job],
    schedules=[schedule for schedule in [stationary_schedule] if schedule is not None],
)

definitions = defs

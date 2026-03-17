from dagster import Definitions

from .jobs import couriers_job
from .schedulers import couriers_schedule


defs = Definitions(
    jobs=[couriers_job],
    schedules=[schedule for schedule in [couriers_schedule] if schedule is not None],
)

definitions = defs

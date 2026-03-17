from dagster import Definitions

from .jobs import birthday_job
from .schedulers import birthday_schedule


defs = Definitions(
    jobs=[birthday_job],
    schedules=[schedule for schedule in [birthday_schedule] if schedule is not None],
)

definitions = defs

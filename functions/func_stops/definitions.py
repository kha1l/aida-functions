from dagster import Definitions

from .jobs import stops_job
from .schedulers import stops_schedule


defs = Definitions(
    jobs=[stops_job],
    schedules=[schedule for schedule in [stops_schedule] if schedule is not None],
)

definitions = defs

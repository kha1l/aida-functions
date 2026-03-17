from dagster import Definitions

from .jobs import staff_job
from .schedulers import staff_schedule


defs = Definitions(
    jobs=[staff_job],
    schedules=[schedule for schedule in [staff_schedule] if schedule is not None],
)

definitions = defs

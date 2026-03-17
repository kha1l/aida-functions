from dagster import Definitions

from .jobs import revenue_job
from .schedulers import revenue_schedule


defs = Definitions(
    jobs=[revenue_job],
    schedules=[schedule for schedule in [revenue_schedule] if schedule is not None],
)

definitions = defs

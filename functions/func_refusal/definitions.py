from dagster import Definitions

from .jobs import refusal_job
from .schedulers import refusal_schedule


defs = Definitions(
    jobs=[refusal_job],
    schedules=[schedule for schedule in [refusal_schedule] if schedule is not None],
)

definitions = defs

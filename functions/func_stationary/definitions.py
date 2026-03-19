from dagster import Definitions

from .jobs import assembly_rest_job, drink_rest_job, later_rest_job, stationary_job
from .schedulers import stationary_schedule


defs = Definitions(
    jobs=[stationary_job, later_rest_job, assembly_rest_job, drink_rest_job],
    schedules=[schedule for schedule in [stationary_schedule] if schedule is not None],
)

definitions = defs

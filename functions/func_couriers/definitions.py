from dagster import Definitions

from .jobs import certificates_job, couriers_job, later_job, longers_job, sectors_job, waiting_job
from .schedulers import couriers_schedule


defs = Definitions(
    jobs=[couriers_job, certificates_job, later_job, waiting_job, sectors_job, longers_job],
    schedules=[schedule for schedule in [couriers_schedule] if schedule is not None],
)

definitions = defs

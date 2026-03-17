from dagster import Definitions

from .jobs import metrics_job
from .schedulers import metrics_schedule


defs = Definitions(
    jobs=[metrics_job],
    schedules=[schedule for schedule in [metrics_schedule] if schedule is not None],
)

definitions = defs

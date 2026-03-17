from dagster import Definitions

from .jobs import stock_job
from .schedulers import stock_schedule


defs = Definitions(
    jobs=[stock_job],
    schedules=[schedule for schedule in [stock_schedule] if schedule is not None],
)

definitions = defs

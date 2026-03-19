from dagster import Definitions

from .jobs import stock_job
from .schedulers import stock_schedule
from functions.shared.definition_resources import COMMON_RESOURCE_DEFS


defs = Definitions(
    jobs=[stock_job],
    schedules=[schedule for schedule in [stock_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

from dagster import Definitions

from functions.shared.definition_resources import COMMON_RESOURCE_DEFS
from .jobs import widget_job
from .schedulers import widget_schedule


defs = Definitions(
    jobs=[widget_job],
    schedules=[schedule for schedule in [widget_schedule] if schedule is not None],
    resources=COMMON_RESOURCE_DEFS,
)

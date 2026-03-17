from dagster import Definitions

from .jobs import tariff_job
from .schedulers import tariff_schedule


defs = Definitions(
    jobs=[tariff_job],
    schedules=[schedule for schedule in [tariff_schedule] if schedule is not None],
)

definitions = defs

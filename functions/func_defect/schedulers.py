"""Расписание для hourly-job по браку."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import defect_job


_cfg = schedule_config("defect", default_cron="0 * * * *")
defect_schedule = None
if _cfg.enabled:
    defect_schedule = ScheduleDefinition(
        job=defect_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

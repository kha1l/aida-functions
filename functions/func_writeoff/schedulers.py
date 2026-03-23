"""Расписание для daily-job по списаниям."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import writeoff_job


_cfg = schedule_config("writeoff", default_cron="0 * * * *")
writeoff_schedule = None
if _cfg.enabled:
    writeoff_schedule = ScheduleDefinition(
        job=writeoff_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

"""Расписание для job по выручке сети."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import revenue_job


_cfg = schedule_config("revenue", default_cron="0 9 * * *")
revenue_schedule = None
if _cfg.enabled:
    revenue_schedule = ScheduleDefinition(
        job=revenue_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

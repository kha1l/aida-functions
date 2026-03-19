"""Расписание для ежедневного restaurant-отчета."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import stationary_job


_cfg = schedule_config("stationary", default_cron="0 9 * * *")
stationary_schedule = None
if _cfg.enabled:
    stationary_schedule = ScheduleDefinition(
        job=stationary_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

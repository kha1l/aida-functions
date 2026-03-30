"""Расписание для job обновления store widget."""

from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import widget_job


_cfg = schedule_config("widget", default_cron="*/10 * * * *", default_timezone="Europe/Moscow")
widget_schedule = None
if _cfg.enabled:
    widget_schedule = ScheduleDefinition(
        job=widget_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

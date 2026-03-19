"""Расписание для hourly-job по отмененным заказам."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import refusal_job


_cfg = schedule_config("refusal", default_cron="0 * * * *")
refusal_schedule = None
if _cfg.enabled:
    refusal_schedule = ScheduleDefinition(
        job=refusal_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

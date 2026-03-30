"""Расписание для job по рейтингам РКО/РС."""

from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import ratings_job


_cfg = schedule_config("ratings", default_cron="0 19 * * *", default_timezone="Europe/Moscow")
ratings_schedule = None
if _cfg.enabled:
    ratings_schedule = ScheduleDefinition(
        job=ratings_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

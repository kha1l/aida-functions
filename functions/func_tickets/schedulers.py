"""Расписание для job по тикетам."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import tickets_job


# Job запускается каждые 5 минут, а внутри берет предыдущее завершенное 5-минутное окно.
_cfg = schedule_config("tickets", default_cron="*/5 * * * *")
tickets_schedule = None
if _cfg.enabled:
    tickets_schedule = ScheduleDefinition(
        job=tickets_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

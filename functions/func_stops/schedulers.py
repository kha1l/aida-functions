"""Расписание для регулярного job по стопам."""
from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import stops_job


# Job запускается каждые 5 минут и берет предыдущее завершенное 5-минутное окно.
_cfg = schedule_config("stops", default_cron="*/5 * * * *")
stops_schedule = None
if _cfg.enabled:
    stops_schedule = ScheduleDefinition(
        job=stops_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

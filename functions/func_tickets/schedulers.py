from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import tickets_job


_cfg = schedule_config("tickets", default_cron="0 9 * * *")
tickets_schedule = None
if _cfg.enabled:
    tickets_schedule = ScheduleDefinition(
        job=tickets_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

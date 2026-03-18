from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import birthday_job


_cfg = schedule_config("birthday", default_cron="0 9 * * *")
birthday_schedule = None
if _cfg.enabled:
    birthday_schedule = ScheduleDefinition(
        job=birthday_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

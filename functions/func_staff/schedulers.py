from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import staff_job


_cfg = schedule_config("staff", default_cron="0 9 * * *")
staff_schedule = None
if _cfg.enabled:
    staff_schedule = ScheduleDefinition(
        job=staff_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

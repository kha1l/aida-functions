from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import couriers_job


_cfg = schedule_config("couriers", default_cron="0 9 * * *")
couriers_schedule = None
if _cfg.enabled:
    couriers_schedule = ScheduleDefinition(
        job=couriers_job,
        cron_schedule=_cfg.cron,
        timezone=_cfg.timezone,
    )

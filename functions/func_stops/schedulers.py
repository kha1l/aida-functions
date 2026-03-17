from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import stops_job


_cfg = schedule_config("stops", default_cron="0 9 * * *")
stops_schedule = None
if _cfg.enabled:
    stops_schedule = ScheduleDefinition(
        job=stops_job,
        cron_schedule=_cfg.cron,
        timezone=_cfg.timezone,
    )

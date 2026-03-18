from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import metrics_job


_cfg = schedule_config("metrics", default_cron="0 9 * * *")
metrics_schedule = None
if _cfg.enabled:
    metrics_schedule = ScheduleDefinition(
        job=metrics_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

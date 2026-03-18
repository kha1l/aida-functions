from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import tariff_job


_cfg = schedule_config("tariff", default_cron="0 9 * * *")
tariff_schedule = None
if _cfg.enabled:
    tariff_schedule = ScheduleDefinition(
        job=tariff_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

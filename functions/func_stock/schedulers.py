from dagster import ScheduleDefinition

from config import schedule_config
from .jobs import stock_job


_cfg = schedule_config("stock", default_cron="0 9 * * *")
stock_schedule = None
if _cfg.enabled:
    stock_schedule = ScheduleDefinition(
        job=stock_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )

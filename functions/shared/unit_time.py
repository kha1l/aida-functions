"""Общие helpers для локального времени точки на основе `special.units.timezone`."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from database.models import Unit


def resolve_unit_now(unit: Optional[Unit]) -> datetime:
    """Возвращает текущее локальное время точки без tzinfo."""
    now_utc = datetime.now(timezone.utc)
    if unit and unit.timezone is not None:
        return (now_utc + timedelta(hours=unit.timezone)).replace(tzinfo=None)
    return datetime.now()


def resolve_unit_today(unit: Optional[Unit]) -> date:
    """Возвращает локальную дату точки."""
    return resolve_unit_now(unit).date()



def floor_unit_to_hour(unit: Optional[Unit]) -> datetime:
    """Округляет локальное время точки вниз до начала часа."""
    return resolve_unit_now(unit).replace(minute=0, second=0, microsecond=0)



def build_previous_local_hour_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает окно предыдущего завершенного локального часа точки."""
    dt_end = floor_unit_to_hour(unit)
    dt_start = dt_end - timedelta(hours=1)
    return dt_start, dt_end



def build_previous_local_day_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает окно предыдущего полного локального дня точки."""
    dt_end = resolve_unit_now(unit).replace(hour=0, minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end

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


def floor_unit_to_minutes(unit: Optional[Unit], *, minutes: int) -> datetime:
    """Округляет локальное время точки вниз до ближайшего кратного интервала."""
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    now = resolve_unit_now(unit).replace(second=0, microsecond=0)
    floored_minute = (now.minute // minutes) * minutes
    return now.replace(minute=floored_minute)



def build_previous_local_hour_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает окно предыдущего завершенного локального часа точки."""
    dt_end = floor_unit_to_hour(unit)
    dt_start = dt_end - timedelta(hours=1)
    return dt_start, dt_end


def build_previous_local_minute_window(
    unit: Optional[Unit],
    *,
    minutes: int,
) -> tuple[datetime, datetime]:
    """Возвращает предыдущее завершенное окно фиксированной длины в локальном времени точки."""
    dt_end = floor_unit_to_minutes(unit, minutes=minutes)
    dt_start = dt_end - timedelta(minutes=minutes)
    return dt_start, dt_end



def build_previous_local_day_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает окно предыдущего полного локального дня точки."""
    dt_end = resolve_unit_now(unit).replace(hour=0, minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end

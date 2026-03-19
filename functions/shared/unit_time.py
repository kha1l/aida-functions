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

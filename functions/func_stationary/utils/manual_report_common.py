"""Общие helper'ы manual-отчетов по ресторану."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from functions.shared.manual_report_runtime import (
    ManualUnitReportBuilder,
    ManualUnitReportRequest,
    build_report_window,
    format_clock,
    html_to_log_text,
    parse_reference_datetime,
    run_manual_unit_report,
    split_text_for_telegram as _shared_split_text_for_telegram,
    to_int,
)


SOURCE_LABELS = {
    "CallCenter": "Колл-центр",
    "Website": "Вэб-сайт",
    "Dine-in": "Ресторан",
    "MobileApp": "Мобильное приложение",
    "Kiosk": "Киоск",
}

ManualStationaryReportRequest = ManualUnitReportRequest
ManualStationaryReportBuilder = ManualUnitReportBuilder


async def run_manual_stationary_report(
    context,
    *,
    post: str,
    log_prefix: str,
    build_report: ManualStationaryReportBuilder,
) -> None:
    await run_manual_unit_report(
        context,
        post=post,
        log_prefix=log_prefix,
        build_report=build_report,
        prefer_paragraph_split=True,
    )


def split_text_for_telegram(text: str) -> list[str]:
    return _shared_split_text_for_telegram(text, prefer_paragraph_split=True)


def parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value or value.startswith("0001-01-01"):
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value.split(".", 1)[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        # Для local-полей Dodo сохраняем локальное время как есть,
        # а не переводим его дополнительно в UTC/таймзону сервера.
        return parsed.replace(tzinfo=None)
    return parsed


def parse_unit_time(
    *,
    utc_value: object,
    local_value: object,
    unit_timezone: Optional[int],
) -> Optional[datetime]:
    parsed_local = parse_dodo_datetime(local_value)
    if parsed_local is not None:
        return parsed_local

    utc_dt = parse_dodo_datetime(utc_value)
    if utc_dt is not None and unit_timezone is not None:
        shifted = utc_dt.replace(tzinfo=timezone.utc).astimezone(
            timezone(timedelta(hours=unit_timezone))
        )
        return shifted.replace(tzinfo=None)
    return utc_dt


def source_label(value: object) -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return SOURCE_LABELS.get(cleaned, cleaned)
    return "Неизвестный источник"


def extract_concept(properties: Optional[str]) -> str:
    if not isinstance(properties, str):
        return "dodopizza"
    parts = [part for part in properties.strip("/").split("/") if part]
    if len(parts) >= 2:
        return parts[1].strip().lower()
    return "dodopizza"

"""Сбор manual-отчета по долгим напиткам."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualStationaryReportRequest,
    build_report_window,
    run_manual_stationary_report,
)


DRINKS_STATION_NAME = "drinks"


@dataclass(frozen=True, slots=True)
class DrinkRestEntry:
    """Одна запись о продукте с долгим средним временем на станции Drinks."""

    product_name: str
    station_avg_duration: str
    total_avg_duration: str
    station_avg_seconds: int


async def run_drink_rest(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по долгим напиткам для одной точки."""
    await run_manual_stationary_report(
        context,
        post="drink_rest",
        log_prefix="drink_rest_op",
        build_report=_build_drink_rest_report,
    )


async def _build_drink_rest_report(
    *,
    dodo_api,
    request: ManualStationaryReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, _dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)
    report_date = dt_start.strftime("%Y-%m-%d")
    threshold_seconds = _request_threshold_seconds(request)
    summary = await dodo_api.get(
        request.account_id,
        "production/tracking-metrics/summary",
        params={
            "units": request.unit_uuid,
            "fromDate": report_date,
            "toDate": report_date,
        },
    )
    if not summary.ok:
        return None, summary.error_html, summary.status

    entries = _collect_drink_rest_entries(
        summary.data if isinstance(summary.data, dict) else {},
        drink_threshold_seconds=threshold_seconds,
    )
    return _format_drink_rest_message(
        unit_name=request.unit_name,
        date_label=date_label,
        drink_threshold_seconds=threshold_seconds,
        entries=entries,
    ), None, 200


def _collect_drink_rest_entries(
    data: Dict[str, object],
    *,
    drink_threshold_seconds: int,
) -> List[DrinkRestEntry]:
    entries: List[DrinkRestEntry] = []

    for item in data.get("result", []) or []:
        if not isinstance(item, dict):
            continue

        drinks_station = _find_drinks_station(item.get("stations"))
        if drinks_station is None:
            continue

        station_avg_duration = str(drinks_station.get("avgDuration") or "00:00:00")
        station_avg_seconds = _parse_hhmmss_to_seconds(station_avg_duration)
        if station_avg_seconds is None or station_avg_seconds <= drink_threshold_seconds:
            continue

        entries.append(
            DrinkRestEntry(
                product_name=str(item.get("productName") or "-"),
                station_avg_duration=station_avg_duration,
                total_avg_duration=str(item.get("avgTotal") or "00:00:00"),
                station_avg_seconds=station_avg_seconds,
            )
        )

    entries.sort(key=lambda entry: entry.station_avg_seconds, reverse=True)
    return entries


def _find_drinks_station(value: object) -> Optional[Dict[str, object]]:
    if not isinstance(value, list):
        return None

    for station in value:
        if not isinstance(station, dict):
            continue
        name = station.get("name")
        if isinstance(name, str) and name.strip().lower() == DRINKS_STATION_NAME:
            return station
    return None


def _format_drink_rest_message(
    *,
    unit_name: str,
    date_label: str,
    drink_threshold_seconds: int,
    entries: List[DrinkRestEntry],
) -> str:
    lines = [
        f"\U0001F37A <b>Долгие напитки за {date_label} в заведении {unit_name}</b>",
        f"Порог времени: больше {_format_threshold(drink_threshold_seconds)}.",
        "",
    ]

    if not entries:
        lines.append(
            "Напитков со средним временем на станции Drinks дольше заданного порога не найдено. Отличная работа, так держать!"
        )
        return "\n".join(lines).rstrip()

    for entry in entries:
        lines.extend(
            [
                f"Название продукта: {entry.product_name}",
                f"Среднее время на станции: {entry.station_avg_duration}",
                # f"Общее среднее время: {entry.total_avg_duration}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _format_threshold(value: int) -> str:
    minutes, seconds = divmod(max(value, 0), 60)
    if seconds == 0:
        minute_word = "мин" if minutes else "сек"
        return f"{minutes} {minute_word}"
    return str(timedelta(seconds=value))


def _parse_hhmmss_to_seconds(value: object) -> Optional[int]:
    if not isinstance(value, str):
        return None
    parts = value.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if hours < 0 or minutes < 0 or seconds < 0:
        return None
    return hours * 3600 + minutes * 60 + seconds


def _request_threshold_seconds(request: ManualStationaryReportRequest) -> int:
    value = request.op_config.get("drink_threshold_seconds")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

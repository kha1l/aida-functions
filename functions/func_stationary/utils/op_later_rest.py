"""Сбор manual-отчета по долгим заказам ресторана."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualStationaryReportRequest,
    build_report_window,
    extract_concept,
    format_clock,
    parse_unit_time,
    run_manual_stationary_report,
    source_label,
    to_int,
)


DRINKIT_CONCEPT = "drinkit"
DRINKIT_LONG_ORDER_THRESHOLD_SECONDS = 6 * 60


@dataclass(frozen=True, slots=True)
class LaterRestEntry:
    """Одна запись о долгом заказе ресторана."""

    order_number: str
    order_time: Optional[datetime]
    tracking_seconds: int
    cooking_seconds: int
    source_label: str
    total_seconds: int


async def run_later_rest(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по долгим заказам ресторана для одной точки."""
    await run_manual_stationary_report(
        context,
        post="later_rest",
        log_prefix="later_rest_op",
        build_report=_build_later_rest_report,
    )


async def _build_later_rest_report(
    *,
    dodo_api,
    request: ManualStationaryReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)
    handover = await dodo_api.get(
        request.account_id,
        "production/orders-handover-time",
        params={
            "units": request.unit_uuid,
            "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
        },
    )
    if not handover.ok:
        return None, handover.error_html, handover.status

    concept = extract_concept(request.account_properties)
    entries = _collect_later_rest_entries(
        handover.data if isinstance(handover.data, dict) else {},
        unit_timezone=request.unit_timezone,
        concept=concept,
    )
    return _format_later_rest_message(
        unit_name=request.unit_name,
        date_label=date_label,
        concept=concept,
        entries=entries,
    ), None, 200


def _collect_later_rest_entries(
    data: Dict[str, object],
    *,
    unit_timezone: Optional[int],
    concept: str,
) -> List[LaterRestEntry]:
    entries: List[LaterRestEntry] = []

    for order in data.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Dine-in":
            continue

        tracking_seconds = to_int(order.get("trackingPendingTime")) or 0
        cooking_seconds = to_int(order.get("cookingTime")) or 0
        total_seconds = tracking_seconds + cooking_seconds
        long_order_threshold_seconds = _long_order_threshold_seconds(
            concept=concept,
            order_number=order.get("orderNumber"),
        )
        if total_seconds <= long_order_threshold_seconds:
            continue

        entries.append(
            LaterRestEntry(
                order_number=str(order.get("orderNumber") or "-"),
                order_time=parse_unit_time(
                    utc_value=order.get("orderTrackingStartAt"),
                    local_value=order.get("orderTrackingStartAtLocal"),
                    unit_timezone=unit_timezone,
                ),
                tracking_seconds=tracking_seconds,
                cooking_seconds=cooking_seconds,
                source_label=source_label(order.get("orderSource")),
                total_seconds=total_seconds,
            )
        )

    entries.sort(key=lambda item: item.total_seconds, reverse=True)
    return entries


def _format_later_rest_message(
    *,
    unit_name: str,
    date_label: str,
    concept: str,
    entries: List[LaterRestEntry],
) -> str:
    lines = [
        f"\u23F1 <b>Долгие заказы за {date_label} в заведении {unit_name}</b>",
    ]

    if concept == DRINKIT_CONCEPT:
        lines.append("Для Drinkit показываем только заказы с общим временем > 6 минут.")

    lines.append("")

    if not entries:
        lines.append("Долгих заказов не найдено. Отличная работа, так держать!")
        return "\n".join(lines).rstrip()

    for entry in entries:
        lines.extend(
            [
                f"Номер заказа: {entry.order_number}",
                f"Время заказа: {format_clock(entry.order_time)}",
                f"Время на трекинге: {timedelta(seconds=entry.tracking_seconds)}",
                f"Время приготовления: {timedelta(seconds=entry.cooking_seconds)}",
                f"Источник заказа: {entry.source_label}",
                f"Общее время: {timedelta(seconds=entry.total_seconds)}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _long_order_threshold_seconds(*, concept: str, order_number: object) -> int:
    if concept == DRINKIT_CONCEPT:
        return DRINKIT_LONG_ORDER_THRESHOLD_SECONDS

    parts_count = _extract_order_parts_count(order_number)
    if parts_count <= 0:
        return 5 * 60
    if parts_count < 4:
        return 15 * 60
    return 25 * 60


def _extract_order_parts_count(order_number: object) -> int:
    if not isinstance(order_number, str) or "-" not in order_number:
        return 0
    try:
        return int(order_number.rsplit("-", 1)[-1])
    except ValueError:
        return 0

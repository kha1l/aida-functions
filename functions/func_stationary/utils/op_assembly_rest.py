from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualStationaryReportRequest,
    build_report_window,
    format_clock,
    parse_unit_time,
    run_manual_stationary_report,
    source_label,
    to_int,
)


ASSEMBLY_THRESHOLD_SECONDS = 60


@dataclass(frozen=True, slots=True)
class AssemblyRestEntry:
    """Одна запись о заказе с долгой сборкой."""

    order_number: str
    order_time: Optional[datetime]
    assembly_seconds: int
    source_label: str


async def run_assembly_rest(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по долгой сборке заказов для одной точки."""
    await run_manual_stationary_report(
        context,
        post="assembly_rest",
        log_prefix="assembly_rest_op",
        build_report=_build_assembly_rest_report,
    )


async def _build_assembly_rest_report(
    *,
    dodo_api,
    request: ManualStationaryReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date)
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

    entries = _collect_assembly_rest_entries(
        handover.data if isinstance(handover.data, dict) else {},
        unit_timezone=request.unit_timezone,
    )
    return _format_assembly_rest_message(
        unit_name=request.unit_name,
        date_label=date_label,
        entries=entries,
    ), None, 200


def _collect_assembly_rest_entries(
    data: Dict[str, object],
    *,
    unit_timezone: Optional[int],
) -> List[AssemblyRestEntry]:
    entries: List[AssemblyRestEntry] = []

    for order in data.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Dine-in":
            continue

        assembly_seconds = to_int(order.get("assemblyTime")) or 0
        if assembly_seconds <= ASSEMBLY_THRESHOLD_SECONDS:
            continue

        entries.append(
            AssemblyRestEntry(
                order_number=str(order.get("orderNumber") or "-"),
                order_time=parse_unit_time(
                    utc_value=order.get("orderTrackingStartAt"),
                    local_value=order.get("orderTrackingStartAtLocal"),
                    unit_timezone=unit_timezone,
                ),
                assembly_seconds=assembly_seconds,
                source_label=source_label(order.get("orderSource")),
            )
        )

    entries.sort(key=lambda item: item.assembly_seconds, reverse=True)
    return entries


def _format_assembly_rest_message(
    *,
    unit_name: str,
    date_label: str,
    entries: List[AssemblyRestEntry],
) -> str:
    lines = [
        f"\U0001F9F3 <b>Долгая сборка заказа за {date_label} в заведении {unit_name}</b>",
        "",
    ]

    if not entries:
        lines.append("Заказов со сборкой дольше 1 минуты не найдено. Отличная работа, так держать!")
        return "\n".join(lines).rstrip()

    for entry in entries:
        lines.extend(
            [
                f"Номер заказа: {entry.order_number}",
                f"Время заказа: {format_clock(entry.order_time)}",
                f"Источник заказа: {entry.source_label}",
                f"Время ожидания сборки: {timedelta(seconds=entry.assembly_seconds)}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()

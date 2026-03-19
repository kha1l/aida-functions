"""Сбор manual-отчета по ожиданиям курьеров."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    format_clock,
    get_staff_name,
    load_staff_name_cache,
    parse_unit_time,
    run_manual_courier_report,
    to_int,
)

PAGE_SIZE = 500


@dataclass(frozen=True, slots=True)
class WaitingEntry:
    courier_name: str
    order_number: str
    ready_at: Optional[datetime]
    assembly_seconds: int
    heated_shelf_seconds: int
    queue_courier: int
    trip_start: Optional[datetime]
    trip_orders_count: int


async def run_waiting(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по ожиданиям для одной точки."""
    await run_manual_courier_report(
        context,
        post="waiting",
        log_prefix="waiting_op",
        build_report=_build_waiting_report,
    )


async def _build_waiting_report(
    *,
    dodo_api,
    request: ManualCourierReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date)
    params_base = {
        "units": request.unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    wait_threshold_seconds = int(request.op_config["wait_threshold_seconds"])

    staff_cache = await load_staff_name_cache(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_uuid=request.unit_uuid,
    )

    entries, error_html, error_status = await _fetch_waiting_entries(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
        staff_cache=staff_cache,
        unit_timezone=request.unit_timezone,
        wait_threshold_seconds=wait_threshold_seconds,
    )
    if entries is None:
        return None, error_html, error_status

    return _format_waiting_message(
        unit_name=request.unit_name,
        date_label=date_label,
        wait_threshold_seconds=wait_threshold_seconds,
        entries=entries,
    ), None, 200


async def _fetch_waiting_entries(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
    staff_cache: Dict[str, str],
    unit_timezone: Optional[int],
    wait_threshold_seconds: int,
) -> tuple[Optional[List[WaitingEntry]], Optional[str], Optional[int]]:
    skip = 0
    entries: List[WaitingEntry] = []

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        orders = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params=params,
        )
        if not orders.ok:
            return None, orders.error_html, orders.status

        data = orders.data if isinstance(orders.data, dict) else {}
        for order in data.get("couriersOrders", []) or []:
            if not isinstance(order, dict):
                continue
            wait = to_int(order.get("orderAssemblyAvgTime"))
            if wait is None or wait <= wait_threshold_seconds:
                continue

            courier_name = await get_staff_name(
                dodo_api=dodo_api,
                account_id=account_id,
                staff_cache=staff_cache,
                staff_id=order.get("courierStaffId"),
            )
            trip_start = parse_unit_time(
                utc_value=order.get("handedOverToDeliveryAt"),
                local_value=order.get("handedOverToDeliveryAtLocal"),
                unit_timezone=unit_timezone,
            )
            heated_shelf_seconds = to_int(order.get("heatedShelfTime")) or 0
            ready_at = (
                trip_start - timedelta(seconds=heated_shelf_seconds)
                if trip_start is not None
                else None
            )

            entries.append(
                WaitingEntry(
                    courier_name=courier_name,
                    order_number=str(order.get("orderNumber") or "-"),
                    ready_at=ready_at,
                    assembly_seconds=wait,
                    heated_shelf_seconds=heated_shelf_seconds,
                    queue_courier=to_int(order.get("numberOfCouriersInQueue")) or 0,
                    trip_start=trip_start,
                    trip_orders_count=to_int(order.get("tripOrdersCount")) or 0,
                )
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    entries.sort(key=lambda item: item.assembly_seconds, reverse=True)
    return entries, None, 200


def _format_waiting_message(
    *,
    unit_name: str,
    date_label: str,
    wait_threshold_seconds: int,
    entries: List[WaitingEntry],
) -> str:
    threshold_label = _format_threshold(wait_threshold_seconds)
    lines = [
        f"\U0001F4A4 <b>Ожидания за {date_label} в заведении {unit_name}</b>",
        f"Порог ожидания: больше {threshold_label}",
        "",
    ]

    if not entries:
        lines.append(
            f"Ожиданий дольше {threshold_label} не найдено. Отличная работа, так держать!"
        )
        return "\n".join(lines).rstrip()

    for entry in entries:
        lines.extend(
            [
                f"\U000026F7 Курьер: {entry.courier_name}",
                f"Номер заказа: {entry.order_number}",
                f"Время готовности заказа: {format_clock(entry.ready_at)}",
                f"Время сборки заказа: {timedelta(seconds=entry.assembly_seconds)}",
                f"Время заказа на полке: {timedelta(seconds=entry.heated_shelf_seconds)}",
                f"Количество курьеров в очереди: {entry.queue_courier}",
                f"Начало поездки: {format_clock(entry.trip_start)}",
                f"Заказов в поездке: {entry.trip_orders_count}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _format_threshold(seconds: int) -> str:
    minutes = max(1, seconds // 60)
    return f"{minutes} мин."

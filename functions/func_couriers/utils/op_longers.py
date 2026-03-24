"""Сбор manual-отчета по долгим доставкам."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    get_staff_name,
    load_staff_name_cache,
    run_manual_courier_report,
    to_int,
)
from .op_couriers import _extract_market_code, _long_delivery_threshold_seconds

PAGE_SIZE = 500


@dataclass(frozen=True, slots=True)
class LongerDeliveryEntry:
    order_number: str
    courier_name: str
    total_time_seconds: int
    delivery_time_seconds: int
    trip_orders_count: int
    is_false_delivery: bool
    sector_name: str
    longer_than_prediction: bool


async def run_longers(context: OpExecutionContext) -> None:
    """Собирает и отправляет подробный отчет по долгим доставкам для одной точки."""
    await run_manual_courier_report(
        context,
        post="longers",
        log_prefix="longers_op",
        build_report=_build_longers_report,
    )


async def _build_longers_report(
    *,
    dodo_api,
    request: ManualCourierReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)
    params_base = {
        "units": request.unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    market_code = _extract_market_code(request.account_properties)

    couriers_orders_task = _fetch_couriers_orders_by_id(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
    )
    handover_task = dodo_api.get(
        request.account_id,
        "production/orders-handover-time",
        params=params_base,
    )
    staff_cache_task = load_staff_name_cache(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_uuid=request.unit_uuid,
    )

    couriers_orders, handover, staff_cache = await asyncio.gather(
        couriers_orders_task,
        handover_task,
        staff_cache_task,
    )

    if couriers_orders[0] is None:
        return None, couriers_orders[1], couriers_orders[2]
    if not handover.ok:
        return None, handover.error_html, handover.status

    entries = await _collect_longer_delivery_entries(
        dodo_api=dodo_api,
        account_id=request.account_id,
        couriers_orders_by_id=couriers_orders[0],
        handover=handover.data if isinstance(handover.data, dict) else {},
        staff_cache=staff_cache,
        market_code=market_code,
    )

    return _format_longers_message(
        unit_name=request.unit_name,
        date_label=date_label,
        entries=entries,
        market_code=market_code,
    ), None, 200


async def _fetch_couriers_orders_by_id(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Dict[str, Dict[str, object]]], Optional[str], Optional[int]]:
    skip = 0
    orders_by_id: Dict[str, Dict[str, object]] = {}

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
            order_id = order.get("orderId")
            if isinstance(order_id, str) and order_id:
                orders_by_id[order_id] = order

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return orders_by_id, None, 200


async def _collect_longer_delivery_entries(
    *,
    dodo_api,
    account_id: int,
    couriers_orders_by_id: Dict[str, Dict[str, object]],
    handover: Dict[str, object],
    staff_cache: Dict[str, str],
    market_code: Optional[str],
) -> List[LongerDeliveryEntry]:
    threshold_seconds = _long_delivery_threshold_seconds(market_code)
    entries: List[LongerDeliveryEntry] = []

    for order in handover.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Delivery":
            continue

        order_id = order.get("orderId")
        if not isinstance(order_id, str) or not order_id:
            continue

        courier_order = couriers_orders_by_id.get(order_id)
        if courier_order is None:
            continue

        delivery_time = to_int(courier_order.get("deliveryTime"))
        if delivery_time is None:
            continue

        tracking_pending_time = to_int(order.get("trackingPendingTime")) or 0
        cooking_time = to_int(order.get("cookingTime")) or 0
        heated_shelf_time = to_int(order.get("heatedShelfTime")) or 0
        total_time_seconds = delivery_time + tracking_pending_time + cooking_time + heated_shelf_time
        if total_time_seconds <= threshold_seconds:
            continue

        courier_name = await get_staff_name(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=courier_order.get("courierStaffId"),
        )

        predicted_delivery_time = to_int(courier_order.get("predictedDeliveryTime"))
        longer_than_prediction = (
            predicted_delivery_time is not None and delivery_time >= predicted_delivery_time
        )

        entries.append(
            LongerDeliveryEntry(
                order_number=str(courier_order.get("orderNumber") or order.get("orderNumber") or "-"),
                courier_name=courier_name,
                total_time_seconds=total_time_seconds,
                delivery_time_seconds=delivery_time,
                trip_orders_count=to_int(courier_order.get("tripOrdersCount")) or 0,
                is_false_delivery=bool(courier_order.get("isFalseDelivery")),
                sector_name=_normalize_sector_name(courier_order.get("sectorName")),
                longer_than_prediction=longer_than_prediction,
            )
        )

    entries.sort(key=lambda item: item.total_time_seconds, reverse=True)
    return entries


def _format_longers_message(
    *,
    unit_name: str,
    date_label: str,
    entries: List[LongerDeliveryEntry],
    market_code: Optional[str],
) -> str:
    threshold_minutes = _long_delivery_threshold_seconds(market_code) // 60
    lines = [
        f"\U0001F6F5 <b>Долгие доставки за {date_label} в заведении {unit_name}</b>",
        f"Порог: больше {threshold_minutes} мин.",
        "",
    ]

    if not entries:
        lines.append("Долгих доставок не найдено. Отличная работа, так держать!")
        return "\n".join(lines).rstrip()

    for entry in entries:
        prediction_text = "Да \U0001F534" if entry.longer_than_prediction else "Нет \U0001F7E2"
        false_delivery_text = "Да" if entry.is_false_delivery else "Нет"
        lines.extend(
            [
                f"Номер заказа: {entry.order_number}",
                f"Курьер: {entry.courier_name}",
                f"Общее время доставки: {timedelta(seconds=entry.total_time_seconds)}",
                f"Фактическое время доставки: {timedelta(seconds=entry.delivery_time_seconds)}",
                f"Заказов в поездке: {entry.trip_orders_count}",
                f"Некорректная доставка: {false_delivery_text}",
                f"Сектор: {entry.sector_name}",
                f"Дольше прогноза: {prediction_text}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _normalize_sector_name(value: object) -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return "Без сектора"

"""Сбор manual-отчета по сертификатам для курьеров."""
from datetime import timedelta
from typing import Dict, Optional

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    format_clock,
    get_staff_name,
    parse_unit_time,
    run_manual_courier_report,
    to_int,
)


async def run_certificates(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по сертификатам для одной точки из op config."""
    await run_manual_courier_report(
        context,
        post="certificates",
        log_prefix="certificates_op",
        build_report=_build_certificates_report,
    )


async def _build_certificates_report(
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

    vouchers = await dodo_api.get(
        request.account_id,
        "delivery/vouchers",
        params={**params_base, "take": 500},
    )
    if not vouchers.ok:
        return None, vouchers.error_html, vouchers.status

    orders_by_id, orders_error_html, orders_error_status = await _fetch_couriers_orders_by_id(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
    )
    if orders_by_id is None:
        return None, orders_error_html or "Не удалось получить заказы курьеров", orders_error_status

    staff_cache: Dict[str, str] = {}
    vouchers_data = vouchers.data if isinstance(vouchers.data, dict) else {}
    report_text = await _format_certificates_message(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_name=request.unit_name,
        unit_timezone=request.unit_timezone,
        date_label=date_label,
        vouchers=vouchers_data,
        orders_by_id=orders_by_id,
        staff_cache=staff_cache,
    )
    return report_text, None, 200


async def _fetch_couriers_orders_by_id(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Dict[str, Dict[str, object]]], Optional[str], Optional[int]]:
    skip = 0
    take = 500
    orders_by_id: Dict[str, Dict[str, object]] = {}

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": take})
        orders = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params=params,
        )
        if not orders.ok:
            return None, orders.error_html, orders.status

        data = orders.data if isinstance(orders.data, dict) else {}
        for order in data.get("couriersOrders", []) or []:
            order_id = order.get("orderId")
            if isinstance(order_id, str) and order_id:
                orders_by_id[order_id] = order

        if data.get("isEndOfListReached"):
            break
        skip += take

    return orders_by_id, None, 200


async def _format_certificates_message(
    *,
    dodo_api,
    account_id: int,
    unit_name: str,
    unit_timezone: Optional[int],
    date_label: str,
    vouchers: Dict[str, object],
    orders_by_id: Dict[str, Dict[str, object]],
    staff_cache: Dict[str, str],
) -> str:
    lines = [f"\U0001F4D1 <b>Сертификаты за {date_label} в заведении {unit_name}</b>", ""]
    found_any = False

    for cert in vouchers.get("vouchers", []) or []:
        if not isinstance(cert, dict):
            continue

        order_id = cert.get("orderId")
        if not isinstance(order_id, str) or not order_id:
            continue

        order = orders_by_id.get(order_id)
        if order is None:
            continue

        found_any = True
        courier_name = await get_staff_name(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=order.get("courierStaffId"),
        )

        number = order.get("orderNumber") or "-"
        order_time = _extract_time(cert.get("orderAcceptedAtLocal"))
        heated_shelf_seconds = to_int(order.get("heatedShelfTime")) or 0
        order_ass_seconds = to_int(order.get("orderAssemblyAvgTime")) or 0
        trip = to_int(order.get("tripOrdersCount")) or 0
        queue_courier = to_int(order.get("numberOfCouriersInQueue")) or 0
        trip_start = parse_unit_time(
            utc_value=order.get("handedOverToDeliveryAt"),
            local_value=order.get("handedOverToDeliveryAtLocal"),
            unit_timezone=unit_timezone,
        )
        ready_at = trip_start - timedelta(seconds=heated_shelf_seconds) if trip_start else None
        predict = "Да \U0001F534"
        predicted_delivery_time = to_int(order.get("predictedDeliveryTime"))
        delivery_time = to_int(order.get("deliveryTime"))
        if (
            predicted_delivery_time is not None
            and delivery_time is not None
            and predicted_delivery_time > delivery_time
        ):
            predict = "Нет \U0001F7E2"

        lines.extend(
            [
                f"\U0001F6B4 Курьер: {courier_name}",
                f"Номер заказа: {number}",
                f"Время заказа: {order_time}",
                f"Время готовности заказа: {format_clock(ready_at)}",
                f"Время заказа на полке: {timedelta(seconds=heated_shelf_seconds)}",
                f"Время сборки заказа: {timedelta(seconds=order_ass_seconds)}",
                f"Количество курьеров в очереди: {queue_courier}",
                f"Начало поездки: {format_clock(trip_start)}",
                f"Поездка дольше прогноза: {predict}",
                f"Заказов в поездке: {trip}",
                "",
            ]
        )

    if not found_any:
        lines.append("Сертификатов не найдено. Отличная работа, так держать!")

    return "\n".join(lines).rstrip()


def _extract_time(value: object) -> str:
    if not isinstance(value, str) or "T" not in value:
        return "-"
    return value.split("T", 1)[1].split(".", 1)[0]

import asyncio
import html
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from messaging import build_payload

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000


async def run_certificates(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по сертификатам для одной точки из op config."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    op_config = context.op_config
    unit_uuid = op_config["unit_uuid"]
    account_id = op_config["account_id"]
    chat_id = op_config["chat_id"]
    topic_id = op_config.get("topic_id")
    message_date = op_config.get("message_date")

    units = await load_units_by_uuids(db, [unit_uuid])
    unit = units.get(unit_uuid)
    unit_name = unit.name if unit and unit.name else unit_uuid
    unit_timezone = unit.timezone if unit else None

    account_auth_map = await load_accounts_and_tokens_bulk(db, {account_id})
    dodo_api.prime_account_token_cache(account_auth_map)

    request_semaphore = None
    if getattr(dodo_api, "max_concurrent_requests", None):
        request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))

    report_text: Optional[str] = None
    last_error_html: Optional[str] = None
    last_error_status: Optional[int] = None

    dodo_timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)
    async with aiohttp.ClientSession(timeout=dodo_timeout, trust_env=True) as dodo_session:
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=request_semaphore,
        )
        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        try:
            report_text, last_error_html, last_error_status = await _build_certificates_report(
                dodo_api=dodo_api,
                unit_uuid=unit_uuid,
                unit_name=unit_name,
                unit_timezone=unit_timezone,
                account_id=account_id,
                message_date=message_date,
            )

            if not report_text:
                if last_error_html:
                    if last_error_status in {401, 403}:
                        context.log.warning(
                            "certificates_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            _html_to_log_text(last_error_html),
                        )
                    elif last_error_status in {429, 500}:
                        context.log.error(
                            "certificates_op: не удалось собрать отчет для unit %s после retry: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                    else:
                        context.log.error(
                            "certificates_op: не удалось собрать отчет для unit %s: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                else:
                    context.log.warning("certificates_op: нет данных для unit %s", unit_uuid)
                return

            for text_chunk in _split_text_for_telegram(report_text):
                payload = build_payload(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    post="certificates",
                    text=text_chunk,
                    unit_uuid=unit_uuid,
                )
                await rabbitmq.publish_async(payload)
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def _build_certificates_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    unit_timezone: Optional[int],
    account_id: int,
    message_date: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    reference_dt = _parse_reference_datetime(message_date) or datetime.now()
    dt_end = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(days=1)
    date_label = dt_start.strftime("%d.%m")

    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    vouchers = await dodo_api.get(
        account_id,
        "delivery/vouchers",
        params={**params_base, "take": 500},
    )
    if not vouchers.ok:
        return None, vouchers.error_html, vouchers.status

    orders_by_id, orders_error_html, orders_error_status = await _fetch_couriers_orders_by_id(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
    )
    if orders_by_id is None:
        return None, orders_error_html or "Не удалось получить заказы курьеров", orders_error_status

    staff_cache: Dict[str, str] = {}
    vouchers_data = vouchers.data if isinstance(vouchers.data, dict) else {}
    report_text = await _format_certificates_message(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_name=unit_name,
        unit_timezone=unit_timezone,
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
        courier_name = await _get_courier_name(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            courier_staff_id=order.get("courierStaffId"),
        )

        number = order.get("orderNumber") or "-"
        order_time = _extract_time(cert.get("orderAcceptedAtLocal"))
        heated_shelf_seconds = _to_int(order.get("heatedShelfTime")) or 0
        order_ass_seconds = _to_int(order.get("orderAssemblyAvgTime")) or 0
        trip = _to_int(order.get("tripOrdersCount")) or 0
        queue_courier = _to_int(order.get("numberOfCouriersInQueue")) or 0
        trip_start = _parse_unit_time(
            utc_value=order.get("handedOverToDeliveryAt"),
            local_value=order.get("handedOverToDeliveryAtLocal"),
            unit_timezone=unit_timezone,
        )
        ready_at = trip_start - timedelta(seconds=heated_shelf_seconds) if trip_start else None
        predict = "Да \U0001F534"
        predicted_delivery_time = _to_int(order.get("predictedDeliveryTime"))
        delivery_time = _to_int(order.get("deliveryTime"))
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
                f"Время готовности заказа: {_format_clock(ready_at)}",
                f"Время заказа на полке: {timedelta(seconds=heated_shelf_seconds)}",
                f"Время сборки заказа: {timedelta(seconds=order_ass_seconds)}",
                f"Количество курьеров в очереди: {queue_courier}",
                f"Начало поездки: {_format_clock(trip_start)}",
                f"Поездка дольше прогноза: {predict}",
                f"Заказов в поездке: {trip}",
                "",
            ]
        )

    if not found_any:
        lines.append("Сертификатов не найдено. Отличная работа, так держать!")

    return "\n".join(lines).rstrip()


async def _get_courier_name(
    *,
    dodo_api,
    account_id: int,
    staff_cache: Dict[str, str],
    courier_staff_id: object,
) -> str:
    if not isinstance(courier_staff_id, str) or not courier_staff_id:
        return "Неизвестный курьер"

    cached = staff_cache.get(courier_staff_id)
    if cached is not None:
        return cached

    courier = await dodo_api.get(account_id, f"staff/members/{courier_staff_id}")
    if not courier.ok or not isinstance(courier.data, dict):
        staff_cache[courier_staff_id] = "Неизвестный курьер"
        return staff_cache[courier_staff_id]

    first_name = courier.data.get("firstName") or ""
    last_name = courier.data.get("lastName") or ""
    full_name = " ".join(part for part in [last_name, first_name] if part).strip()
    staff_cache[courier_staff_id] = full_name or "Неизвестный курьер"
    return staff_cache[courier_staff_id]


def _split_text_for_telegram(text: str) -> List[str]:
    if len(text) <= TELEGRAM_MESSAGE_LIMIT:
        return [text]

    chunks: List[str] = []
    current = text
    while len(current) > SAFE_TELEGRAM_CHUNK:
        split_at = current.rfind("\n", 0, SAFE_TELEGRAM_CHUNK)
        if split_at <= 0:
            split_at = SAFE_TELEGRAM_CHUNK
        chunks.append(current[:split_at].rstrip())
        current = current[split_at:].lstrip()
    if current:
        chunks.append(current)
    return chunks


def _html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _to_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_time(value: object) -> str:
    if not isinstance(value, str) or "T" not in value:
        return "-"
    return value.split("T", 1)[1].split(".", 1)[0]


def _parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return datetime.strptime(value.split(".", 1)[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def _parse_reference_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_unit_time(
    *,
    utc_value: object,
    local_value: object,
    unit_timezone: Optional[int],
) -> Optional[datetime]:
    parsed_utc = _parse_dodo_datetime(utc_value)
    if parsed_utc is not None and unit_timezone is not None:
        return parsed_utc + timedelta(hours=unit_timezone)

    parsed_local = _parse_dodo_datetime(local_value)
    if parsed_local is not None:
        return parsed_local

    return parsed_utc


def _format_clock(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.time().replace(microsecond=0).isoformat()

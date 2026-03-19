from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from messaging import build_payload

from .op_couriers import _extract_market_code, _long_delivery_threshold_seconds

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000
PAGE_SIZE = 500


@dataclass(frozen=True, slots=True)
class CourierProfile:
    full_name: str


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

    account_auth_map = await load_accounts_and_tokens_bulk(db, {account_id})
    dodo_api.prime_account_token_cache(account_auth_map)

    account, _ = account_auth_map.get(account_id, (None, None))
    market_code = _extract_market_code(account.properties if account else None)

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
            report_text, last_error_html, last_error_status = await _build_longers_report(
                dodo_api=dodo_api,
                unit_uuid=unit_uuid,
                unit_name=unit_name,
                account_id=account_id,
                message_date=message_date,
                market_code=market_code,
            )

            if not report_text:
                if last_error_html:
                    if last_error_status in {401, 403}:
                        context.log.warning(
                            "longers_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            _html_to_log_text(last_error_html),
                        )
                    elif last_error_status in {429, 500}:
                        context.log.error(
                            "longers_op: не удалось собрать отчет для unit %s после retry: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                    else:
                        context.log.error(
                            "longers_op: не удалось собрать отчет для unit %s: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                else:
                    context.log.warning("longers_op: нет данных для unit %s", unit_uuid)
                return

            for text_chunk in _split_text_for_telegram(report_text):
                payload = build_payload(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    post="longers",
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


async def _build_longers_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    message_date: Optional[str],
    market_code: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = _build_report_window(message_date)
    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    couriers_orders_task = _fetch_couriers_orders_by_id(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
    )
    handover_task = dodo_api.get(
        account_id,
        "production/orders-handover-time",
        params=params_base,
    )
    staff_cache_task = _load_staff_cache(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
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
        account_id=account_id,
        couriers_orders_by_id=couriers_orders[0],
        handover=handover.data if isinstance(handover.data, dict) else {},
        staff_cache=staff_cache,
        market_code=market_code,
    )

    return _format_longers_message(
        unit_name=unit_name,
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


async def _load_staff_cache(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
) -> Dict[str, CourierProfile]:
    members_result = await dodo_api.get(
        account_id,
        "staff/members",
        params={
            "units": unit_uuid,
            "take": 990,
        },
    )
    if not members_result.ok or not isinstance(members_result.data, dict):
        return {}

    staff_cache: Dict[str, CourierProfile] = {}
    for member in members_result.data.get("members", []) or []:
        if not isinstance(member, dict):
            continue
        staff_id = member.get("id") or member.get("staffId")
        if not isinstance(staff_id, str) or not staff_id:
            continue
        staff_cache[staff_id] = CourierProfile(
            full_name=_format_staff_name(member, fallback=staff_id)
        )
    return staff_cache


async def _collect_longer_delivery_entries(
    *,
    dodo_api,
    account_id: int,
    couriers_orders_by_id: Dict[str, Dict[str, object]],
    handover: Dict[str, object],
    staff_cache: Dict[str, CourierProfile],
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

        delivery_time = _to_int(courier_order.get("deliveryTime"))
        if delivery_time is None:
            continue

        tracking_pending_time = _to_int(order.get("trackingPendingTime")) or 0
        cooking_time = _to_int(order.get("cookingTime")) or 0
        heated_shelf_time = _to_int(order.get("heatedShelfTime")) or 0
        total_time_seconds = delivery_time + tracking_pending_time + cooking_time + heated_shelf_time
        if total_time_seconds <= threshold_seconds:
            continue

        courier_name = await _get_courier_name(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            courier_staff_id=courier_order.get("courierStaffId"),
        )

        predicted_delivery_time = _to_int(courier_order.get("predictedDeliveryTime"))
        longer_than_prediction = (
            predicted_delivery_time is not None and delivery_time >= predicted_delivery_time
        )

        entries.append(
            LongerDeliveryEntry(
                order_number=str(courier_order.get("orderNumber") or order.get("orderNumber") or "-"),
                courier_name=courier_name,
                total_time_seconds=total_time_seconds,
                delivery_time_seconds=delivery_time,
                trip_orders_count=_to_int(courier_order.get("tripOrdersCount")) or 0,
                is_false_delivery=bool(courier_order.get("isFalseDelivery")),
                sector_name=_normalize_sector_name(courier_order.get("sectorName")),
                longer_than_prediction=longer_than_prediction,
            )
        )

    entries.sort(key=lambda item: item.total_time_seconds, reverse=True)
    return entries


async def _get_courier_name(
    *,
    dodo_api,
    account_id: int,
    staff_cache: Dict[str, CourierProfile],
    courier_staff_id: object,
) -> str:
    if not isinstance(courier_staff_id, str) or not courier_staff_id:
        return "Неизвестный курьер"

    cached = staff_cache.get(courier_staff_id)
    if cached is not None:
        return cached.full_name

    courier = await dodo_api.get(account_id, f"staff/members/{courier_staff_id}")
    if not courier.ok or not isinstance(courier.data, dict):
        staff_cache[courier_staff_id] = CourierProfile(full_name="Неизвестный курьер")
        return staff_cache[courier_staff_id].full_name

    profile = CourierProfile(
        full_name=_format_staff_name(courier.data, fallback=courier_staff_id)
    )
    staff_cache[courier_staff_id] = profile
    return profile.full_name


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


def _build_report_window(message_date: Optional[str]) -> Tuple[datetime, datetime, str]:
    reference_dt = _parse_reference_datetime(message_date) or datetime.now()
    dt_end = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end, dt_start.strftime("%d.%m")


def _html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _format_staff_name(member: Dict[str, object], *, fallback: str) -> str:
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = " ".join(part for part in [last_name, first_name] if part).strip()
    return full_name or f"Сотрудник {fallback}"


def _normalize_sector_name(value: object) -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return "Без сектора"


def _to_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_reference_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed

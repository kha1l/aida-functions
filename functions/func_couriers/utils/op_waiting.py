from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from messaging import build_payload

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000
PAGE_SIZE = 500


@dataclass(frozen=True, slots=True)
class CourierProfile:
    full_name: str


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
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    op_config = context.op_config
    unit_uuid = op_config["unit_uuid"]
    account_id = op_config["account_id"]
    chat_id = op_config["chat_id"]
    topic_id = op_config.get("topic_id")
    message_date = op_config.get("message_date")
    wait_threshold_seconds = op_config["wait_threshold_seconds"]

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
            report_text, last_error_html, last_error_status = await _build_waiting_report(
                dodo_api=dodo_api,
                unit_uuid=unit_uuid,
                unit_name=unit_name,
                unit_timezone=unit_timezone,
                account_id=account_id,
                message_date=message_date,
                wait_threshold_seconds=wait_threshold_seconds,
            )

            if not report_text:
                if last_error_html:
                    if last_error_status in {401, 403}:
                        context.log.warning(
                            "waiting_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            _html_to_log_text(last_error_html),
                        )
                    elif last_error_status in {429, 500}:
                        context.log.error(
                            "waiting_op: не удалось собрать отчет для unit %s после retry: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                    else:
                        context.log.error(
                            "waiting_op: не удалось собрать отчет для unit %s: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                else:
                    context.log.warning("waiting_op: нет данных для unit %s", unit_uuid)
                return

            for text_chunk in _split_text_for_telegram(report_text):
                payload = build_payload(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    post="waiting",
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


async def _build_waiting_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    unit_timezone: Optional[int],
    account_id: int,
    message_date: Optional[str],
    wait_threshold_seconds: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = _build_report_window(message_date)
    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    staff_cache = await _load_staff_cache(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
    )

    entries, error_html, error_status = await _fetch_waiting_entries(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
        staff_cache=staff_cache,
        unit_timezone=unit_timezone,
        wait_threshold_seconds=wait_threshold_seconds,
    )
    if entries is None:
        return None, error_html, error_status

    return _format_waiting_message(
        unit_name=unit_name,
        date_label=date_label,
        wait_threshold_seconds=wait_threshold_seconds,
        entries=entries,
    ), None, 200


async def _fetch_waiting_entries(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
    staff_cache: Dict[str, CourierProfile],
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
            wait = _to_int(order.get("orderAssemblyAvgTime"))
            if wait is None or wait <= wait_threshold_seconds:
                continue

            courier_name = await _get_courier_name(
                dodo_api=dodo_api,
                account_id=account_id,
                staff_cache=staff_cache,
                courier_staff_id=order.get("courierStaffId"),
            )
            trip_start = _parse_unit_time(
                utc_value=order.get("handedOverToDeliveryAt"),
                local_value=order.get("handedOverToDeliveryAtLocal"),
                unit_timezone=unit_timezone,
            )
            heated_shelf_seconds = _to_int(order.get("heatedShelfTime")) or 0
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
                    queue_courier=_to_int(order.get("numberOfCouriersInQueue")) or 0,
                    trip_start=trip_start,
                    trip_orders_count=_to_int(order.get("tripOrdersCount")) or 0,
                )
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    entries.sort(key=lambda item: item.assembly_seconds, reverse=True)
    return entries, None, 200


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
                f"Время готовности заказа: {_format_clock(entry.ready_at)}",
                f"Время сборки заказа: {timedelta(seconds=entry.assembly_seconds)}",
                f"Время заказа на полке: {timedelta(seconds=entry.heated_shelf_seconds)}",
                f"Количество курьеров в очереди: {entry.queue_courier}",
                f"Начало поездки: {_format_clock(entry.trip_start)}",
                f"Заказов в поездке: {entry.trip_orders_count}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _build_report_window(message_date: Optional[str]) -> tuple[datetime, datetime, str]:
    reference_dt = _parse_reference_datetime(message_date) or datetime.now()
    dt_end = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end, dt_start.strftime("%d.%m")


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


def _format_staff_name(member: Dict[str, object], *, fallback: str) -> str:
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = " ".join(part for part in [last_name, first_name] if part).strip()
    return full_name or f"Сотрудник {fallback}"


def _format_threshold(seconds: int) -> str:
    minutes = max(1, seconds // 60)
    return f"{minutes} мин."


def _to_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
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
        return parsed.astimezone().replace(tzinfo=None)
    return parsed


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

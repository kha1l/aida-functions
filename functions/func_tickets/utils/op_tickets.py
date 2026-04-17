"""Регулярный job по клиентским тикетам из Pyrus."""

from __future__ import annotations

import asyncio
import html
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dagster import OpExecutionContext

from database.models import Unit
from database.requests import select_report_subscriptions
from functions.func_couriers.utils.manual_report_common import (
    get_staff_name,
    load_staff_name_cache,
)
from functions.shared.report_job_runtime import (
    ReportJobSnapshot,
    load_report_job_snapshot,
    publish_report_to_subscribers,
    report_job_runtime,
)

TASK_FIELD_IDS = (198, 284, 285, 286, 280, 13)
MAX_CONCURRENT_TASKS = 10
ORDER_UUID_FIELD_ID = 303
RATING_FIELD_ID = 142
COMMENT_FIELD_ID = 143


@dataclass(slots=True)
class TicketItem:
    """Клиентский тикет из Pyrus, уже привязанный к конкретной точке."""

    task_id: int
    unit_uuid: str
    unit_name: str
    rating: Optional[int]
    order_number: str
    order_type: str
    comment: str
    order_date: Optional[str]
    order_time: Optional[str]
    order_uuid: Optional[str]
    channel_name: Optional[str]
    courier_name: Optional[str]
    created_at: Optional[datetime]


async def run_tickets(context: OpExecutionContext) -> None:
    """Собирает тикеты из Pyrus за последние 5 минут и рассылает их подписчикам."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    pyrus = context.resources.pyrus

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_resource: select_report_subscriptions(db_resource, "tickets"),
    )
    if not snapshot.subscriptions:
        context.log.info("tickets_op: нет подписок")
        return

    if not snapshot.units:
        context.log.warning("tickets_op: не удалось найти units для подписок")
        return

    utc_start, utc_end = _build_utc_window()
    publish_lock = asyncio.Lock()
    dodo_api = context.resources.dodo_api

    async with report_job_runtime(dodo_api, rabbitmq), AsyncExitStack() as stack:
        dodo_api.prime_account_token_cache(snapshot.account_auth_map)

        pyrus_request_semaphore = asyncio.Semaphore(max(1, pyrus.max_concurrent_requests))
        timeout = aiohttp.ClientTimeout(total=pyrus.timeout_seconds)
        session = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=timeout, trust_env=True)
        )
        pyrus.configure_runtime(
            session=session,
            request_semaphore=pyrus_request_semaphore,
        )

        try:
            catalog_map = await _load_catalog_map(
                context,
                pyrus,
                snapshot.subs_by_unit.keys(),
            )
            if not catalog_map:
                return

            tasks = await pyrus.search_form_tasks(
                created_after=utc_start,
                created_before=utc_end,
                catalog_item_ids=catalog_map.values(),
                field_ids=TASK_FIELD_IDS,
            )
            if not tasks:
                context.log.info(
                    "tickets_op: за окно %s - %s тикетов нет", utc_start, utc_end
                )
                return

            tickets_by_unit = await _collect_tickets_by_unit(
                context=context,
                pyrus=pyrus,
                dodo_api=dodo_api,
                tasks=tasks,
                snapshot=snapshot,
            )
            if not tickets_by_unit:
                context.log.info(
                    "tickets_op: за окно %s - %s нет тикетов с комментариями",
                    utc_start,
                    utc_end,
                )
                return

            sorted_unit_ids = sorted(
                tickets_by_unit.keys(),
                key=lambda item: snapshot.unit_name_for(item),
            )
            for unit_uuid in sorted_unit_ids:
                tickets = tickets_by_unit.get(unit_uuid, [])
                if not tickets:
                    continue
                subscribers = snapshot.subscribers_for(unit_uuid)
                if not subscribers:
                    continue
                text = _format_tickets_message(
                    snapshot.unit_for(unit_uuid),
                    tickets,
                    utc_start,
                    utc_end,
                )
                await publish_report_to_subscribers(
                    rabbitmq,
                    publish_lock=publish_lock,
                    subscribers=subscribers,
                    post="tickets",
                    text=text,
                    unit_uuid=unit_uuid,
                    prefer_paragraph_split=True,
                )
        finally:
            pyrus.reset_runtime()


async def _load_catalog_map(
    context: OpExecutionContext,
    pyrus,
    unit_uuids,
) -> Dict[str, int]:
    """Грузит каталог пиццерий из Pyrus и строит `unit_uuid -> item_id`."""
    items = await pyrus.get_catalog_items()
    result: Dict[str, int] = {}
    for item in items:
        values = item.get("values")
        item_id = item.get("item_id")
        if not isinstance(values, list) or not values or not item_id:
            continue
        unit_uuid = str(values[0] or "").strip().lower()
        if unit_uuid:
            result[unit_uuid] = int(item_id)

    filtered = {
        unit_uuid: result[unit_uuid]
        for unit_uuid in unit_uuids
        if unit_uuid in result
    }
    missing = sorted(set(unit_uuids) - set(filtered.keys()))
    if missing:
        context.log.warning(
            "tickets_op: часть заведений не найдена в каталоге Pyrus: %s",
            ", ".join(missing),
        )
    return filtered


async def _collect_tickets_by_unit(
    *,
    context: OpExecutionContext,
    pyrus,
    dodo_api,
    tasks: List[Dict[str, Any]],
    snapshot: ReportJobSnapshot,
) -> Dict[str, List[TicketItem]]:
    """Дочитывает карточки задач Pyrus и группирует тикеты по unit_uuid."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    grouped: Dict[str, List[TicketItem]] = {}
    courier_orders_cache: Dict[Tuple[str, str, int], Dict[str, Dict[str, Any]]] = {}
    staff_cache_by_account: Dict[Tuple[str, int], Dict[str, str]] = {}

    async def process_task(task_preview: Dict[str, Any]) -> Optional[TicketItem]:
        task_id = _to_int(task_preview.get("id"))
        if task_id is None:
            return None
        async with semaphore:
            try:
                task = await pyrus.get_task(task_id)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(
                    "tickets_op: не удалось прочитать task %s из Pyrus: %s",
                    task_id,
                    exc,
                )
                return None
        ticket = _parse_ticket(
            task_id=task_id,
            task=task,
            units=snapshot.units,
            subscribed_units=set(snapshot.subs_by_unit.keys()),
        )
        if ticket is None or not ticket.comment:
            return None

        ticket.courier_name = await _resolve_ticket_courier_name(
            context=context,
            dodo_api=dodo_api,
            snapshot=snapshot,
            ticket=ticket,
            courier_orders_cache=courier_orders_cache,
            staff_cache_by_account=staff_cache_by_account,
        )
        return ticket

    parsed = await asyncio.gather(
        *(process_task(task_preview) for task_preview in tasks)
    )
    for ticket in parsed:
        if ticket is None:
            continue
        grouped.setdefault(ticket.unit_uuid, []).append(ticket)

    for unit_uuid in list(grouped.keys()):
        grouped[unit_uuid].sort(
            key=lambda item: (
                item.created_at or datetime.min.replace(tzinfo=timezone.utc),
                item.task_id,
            )
        )
    return grouped


def _parse_ticket(
    *,
    task_id: int,
    task: Dict[str, Any],
    units: Dict[str, Unit],
    subscribed_units: set[str],
) -> Optional[TicketItem]:
    """Разбирает нужные поля из полной карточки задачи Pyrus."""
    fields = task.get("fields")
    if not isinstance(fields, list):
        return None

    fields_by_id = {
        field.get("id"): field
        for field in fields
        if isinstance(field, dict) and field.get("id") is not None
    }

    catalog_field = fields_by_id.get(198)
    catalog_value = (
        catalog_field.get("value") if isinstance(catalog_field, dict) else None
    )
    catalog_values = (
        catalog_value.get("values") if isinstance(catalog_value, dict) else None
    )
    unit_uuid = (
        str(catalog_values[0] or "").strip().lower()
        if isinstance(catalog_values, list) and catalog_values
        else ""
    )
    if not unit_uuid or unit_uuid not in subscribed_units:
        return None

    unit = units.get(unit_uuid)
    unit_name = (
        unit.name if unit and unit.name else _safe_catalog_unit_name(catalog_values)
    )

    channel_field = fields_by_id.get(13)
    channel_value = (
        channel_field.get("value") if isinstance(channel_field, dict) else None
    )
    nested_fields = (
        channel_value.get("fields") if isinstance(channel_value, dict) else None
    )
    customer_index_field = fields_by_id.get(261)
    customer_index_value = (
        customer_index_field.get("value")
        if isinstance(customer_index_field, dict)
        else None
    )
    customer_index_fields = (
        customer_index_value.get("fields")
        if isinstance(customer_index_value, dict)
        else None
    )

    rating = _to_int(_find_nested_value(nested_fields, RATING_FIELD_ID))
    comment = str(_find_nested_value(nested_fields, COMMENT_FIELD_ID) or "").strip()
    if not comment:
        return None

    return TicketItem(
        task_id=task_id,
        unit_uuid=unit_uuid,
        unit_name=unit_name,
        rating=rating,
        order_number=_string_value(fields_by_id.get(284)) or "Не указан",
        order_type=_extract_choice_name(fields_by_id.get(280)) or "Не указан",
        comment=comment,
        order_date=_string_value(fields_by_id.get(285)),
        order_time=_string_value(fields_by_id.get(286)),
        order_uuid=_string_value_by_nested_id(customer_index_fields, ORDER_UUID_FIELD_ID),
        channel_name=_extract_choice_name(channel_field),
        courier_name=None,
        created_at=_parse_utc_datetime(task.get("create_date")),
    )


async def _resolve_ticket_courier_name(
    *,
    context: OpExecutionContext,
    dodo_api,
    snapshot: ReportJobSnapshot,
    ticket: TicketItem,
    courier_orders_cache: Dict[Tuple[str, str, int], Dict[str, Dict[str, Any]]],
    staff_cache_by_account: Dict[Tuple[str, int], Dict[str, str]],
) -> Optional[str]:
    """Пытается определить курьера по OrderUUId из Pyrus через Dodo API."""
    if ticket.order_type != "Доставка":
        return None
    if not ticket.order_uuid or not ticket.order_date:
        return None

    normalized_order_uuid = ticket.order_uuid.lower()
    candidate_accounts = snapshot.candidate_accounts_for(ticket.unit_uuid)
    if not candidate_accounts:
        return None

    for account_id in candidate_accounts:
        orders_by_id = await _load_delivery_orders_for_date(
            context=context,
            dodo_api=dodo_api,
            unit_uuid=ticket.unit_uuid,
            account_id=account_id,
            order_date=ticket.order_date,
            cache=courier_orders_cache,
        )
        order = orders_by_id.get(normalized_order_uuid)
        if order is None:
            continue

        cache_key = (ticket.unit_uuid, account_id)
        staff_cache = staff_cache_by_account.get(cache_key)
        if staff_cache is None:
            staff_cache = await load_staff_name_cache(
                dodo_api=dodo_api,
                account_id=account_id,
                unit_uuid=ticket.unit_uuid,
            )
            staff_cache_by_account[cache_key] = staff_cache

        return await get_staff_name(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=order.get("courierStaffId"),
        )

    return None


async def _load_delivery_orders_for_date(
    *,
    context: OpExecutionContext,
    dodo_api,
    unit_uuid: str,
    account_id: int,
    order_date: str,
    cache: Dict[Tuple[str, str, int], Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    """Грузит delivery/couriers-orders за дату заказа и кеширует по точке/дате/аккаунту."""
    cache_key = (unit_uuid, order_date, account_id)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    skip = 0
    take = 500
    orders_by_id: Dict[str, Dict[str, Any]] = {}

    while True:
        result = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params={
                "units": unit_uuid,
                "from": f"{order_date}T00:00:00",
                "to": f"{order_date}T23:59:59",
                "skip": skip,
                "take": take,
            },
        )
        if not result.ok:
            context.log.warning(
                "tickets_op: не удалось получить заказы курьеров для unit %s, account_id=%s, date=%s: %s",
                unit_uuid,
                account_id,
                order_date,
                result.error_html or result.status,
            )
            cache[cache_key] = {}
            return cache[cache_key]

        data = result.data if isinstance(result.data, dict) else {}
        for order in data.get("couriersOrders", []) or []:
            if not isinstance(order, dict):
                continue
            order_id = str(order.get("orderId") or "").strip().lower()
            if order_id:
                orders_by_id[order_id] = order

        if data.get("isEndOfListReached"):
            break
        skip += take

    cache[cache_key] = orders_by_id
    return orders_by_id


def _format_tickets_message(
    unit: Optional[Unit],
    tickets: List[TicketItem],
    utc_start: datetime,
    utc_end: datetime,
) -> str:
    """Собирает итоговый текст отчета по тикетам для одной точки."""
    unit_name = (
        tickets[0].unit_name
        if tickets
        else (unit.name if unit and unit.name else "Неизвестное заведение")
    )
    local_start, local_end = _to_unit_local_window(unit, utc_start, utc_end)

    lines = [
        f"📬 <b>Тикеты клиентов в {html.escape(unit_name)}</b>",
        f"Период: {local_start.strftime('%H:%M')} - {local_end.strftime('%H:%M')}",
        "",
    ]
    for ticket in tickets:
        lines.append(f"Оценка: {ticket.rating if ticket.rating is not None else '-'}")
        lines.append(f"Номер заказа: {html.escape(ticket.order_number)}")
        lines.append(f"Тип заказа: {html.escape(ticket.order_type)}")
        lines.append(
            f"Канал обращения: {html.escape(ticket.channel_name or 'Не указан')}"
        )
        if ticket.order_type == "Доставка":
            lines.append(f"Курьер: {_format_courier_name(ticket.courier_name)}")
        lines.append(f"Комментарий: {html.escape(ticket.comment)}")
        lines.append(
            f"Дата и время заказа: {html.escape(_format_order_datetime(unit, ticket.order_date, ticket.order_time))}"
        )
        lines.append(
            f'Тикет Pyrus: <a href="{html.escape(_build_pyrus_task_url(ticket.task_id), quote=True)}">открыть</a>'
        )
        lines.append("")
    lines.append("#тикеты")
    return "\n".join(lines).strip()


def _format_courier_name(value: Optional[str]) -> str:
    """Экранирует ФИО курьера и выделяет его жирным, если оно найдено."""
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return f"<b>{html.escape(cleaned)}</b>"
    return html.escape("Не удалось определить")


def _build_utc_window(now_utc: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """Строит предыдущее завершенное 5-минутное окно в UTC."""
    now_utc = now_utc or datetime.now(timezone.utc)
    minute_bucket = (now_utc.minute // 5) * 5
    dt_end = now_utc.replace(minute=minute_bucket, second=0, microsecond=0)
    dt_start = dt_end - timedelta(minutes=5)
    return dt_start, dt_end


def _to_unit_local_window(
    unit: Optional[Unit],
    utc_start: datetime,
    utc_end: datetime,
) -> tuple[datetime, datetime]:
    """Переводит UTC-окно в локальное время точки для человекочитаемого отчета."""
    offset = (
        timedelta(hours=unit.timezone)
        if unit and unit.timezone is not None
        else timedelta(0)
    )
    local_start = (utc_start + offset).replace(tzinfo=None)
    local_end = (utc_end + offset).replace(tzinfo=None)
    return local_start, local_end


def _find_nested_value(fields: Any, field_id: int) -> Any:
    """Ищет значение во вложенных полях Pyrus по id."""
    if not isinstance(fields, list):
        return None
    for field in fields:
        if not isinstance(field, dict) or field.get("id") != field_id:
            continue
        return field.get("value")
    return None


def _extract_choice_name(field: Any) -> Optional[str]:
    """Достает первое choice_name из поля Pyrus."""
    if not isinstance(field, dict):
        return None
    value = field.get("value")
    if not isinstance(value, dict):
        return None
    names = value.get("choice_names")
    if isinstance(names, list) and names:
        return str(names[0]).strip() or None
    return None


def _string_value(field: Any) -> Optional[str]:
    """Нормализует простое значение поля Pyrus к строке."""
    if not isinstance(field, dict):
        return None
    value = field.get("value")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_value_by_nested_id(fields: Any, field_id: int) -> Optional[str]:
    """Нормализует значение вложенного поля Pyrus к строке по его id."""
    value = _find_nested_value(fields, field_id)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_catalog_unit_name(values: Any) -> str:
    """Пытается достать имя заведения из каталожного поля Pyrus."""
    if isinstance(values, list) and len(values) > 1:
        name = str(values[1]).strip()
        if name:
            return name
    return "Неизвестное заведение"


def _format_order_datetime(
    unit: Optional[Unit],
    order_date: Optional[str],
    order_time: Optional[str],
) -> str:
    """Форматирует UTC дату/время заказа в локальное время заведения."""
    if not order_date and not order_time:
        return "Не указаны"

    parsed_utc = _parse_order_utc_datetime(order_date, order_time)
    if parsed_utc is not None:
        offset = (
            timedelta(hours=unit.timezone)
            if unit and unit.timezone is not None
            else timedelta(0)
        )
        local_dt = (parsed_utc + offset).replace(tzinfo=None)
        return local_dt.strftime("%d.%m.%Y %H:%M")

    if order_date:
        try:
            parsed_date = datetime.strptime(order_date, "%Y-%m-%d")
            formatted_date = parsed_date.strftime("%d.%m.%Y")
        except ValueError:
            formatted_date = order_date
    else:
        formatted_date = ""

    return " ".join(part for part in [formatted_date, order_time] if part).strip() or "Не указаны"


def _parse_order_utc_datetime(
    order_date: Optional[str],
    order_time: Optional[str],
) -> Optional[datetime]:
    """Пытается собрать UTC datetime из отдельных полей Pyrus."""
    if order_time and "T" in order_time:
        return _parse_utc_datetime(order_time)

    if not order_date or not order_time:
        return None

    cleaned_time = order_time.strip()
    if not cleaned_time:
        return None
    if len(cleaned_time.split(":")) == 2:
        cleaned_time = f"{cleaned_time}:00"

    normalized = f"{order_date.strip()}T{cleaned_time}".replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc)
    return parsed.replace(tzinfo=timezone.utc)


def _build_pyrus_task_url(task_id: int) -> str:
    """Строит прямую ссылку на карточку тикета в веб-интерфейсе Pyrus."""
    return f"https://pyrus.com/t#id{task_id}"


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    """Парсит UTC-datetime из строки Pyrus."""
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError:
        return None


def _to_int(value: Any) -> Optional[int]:
    """Безопасно приводит значение к int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

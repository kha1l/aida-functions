from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from messaging import build_payload

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000
PAGE_SIZE = 500
UNKNOWN_SECTOR_NAME = "Без сектора"


@dataclass(frozen=True, slots=True)
class SectorStats:
    orders_count: int
    revenue: Decimal
    stop_seconds: int


async def run_sectors(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по секторам доставки для одной точки."""
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
            report_text, last_error_html, last_error_status = await _build_sectors_report(
                dodo_api=dodo_api,
                unit_uuid=unit_uuid,
                unit_name=unit_name,
                account_id=account_id,
                message_date=message_date,
            )

            if not report_text:
                if last_error_html:
                    if last_error_status in {401, 403}:
                        context.log.warning(
                            "sectors_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            _html_to_log_text(last_error_html),
                        )
                    elif last_error_status in {429, 500}:
                        context.log.error(
                            "sectors_op: не удалось собрать отчет для unit %s после retry: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                    else:
                        context.log.error(
                            "sectors_op: не удалось собрать отчет для unit %s: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                else:
                    context.log.warning("sectors_op: нет данных для unit %s", unit_uuid)
                return

            for text_chunk in _split_text_for_telegram(report_text):
                payload = build_payload(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    post="sectors",
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


async def _build_sectors_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    message_date: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = _build_report_window(message_date)
    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    sectors_by_order_task = _fetch_delivery_orders_by_sector(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
    )
    revenue_by_order_task = _fetch_delivery_revenue_by_order(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
    )
    stops_by_sector_task = _fetch_stop_seconds_by_sector(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
        dt_start=dt_start,
        dt_end=dt_end,
    )

    sectors_by_order, revenue_by_order, stops_by_sector = await asyncio.gather(
        sectors_by_order_task,
        revenue_by_order_task,
        stops_by_sector_task,
    )

    for result in (sectors_by_order, revenue_by_order, stops_by_sector):
        if result[0] is None:
            return None, result[1], result[2]

    stats_by_sector = _merge_sector_stats(
        sectors_by_order=sectors_by_order[0],
        revenue_by_order=revenue_by_order[0],
        stops_by_sector=stops_by_sector[0],
    )
    return _format_sectors_message(
        unit_name=unit_name,
        date_label=date_label,
        stats_by_sector=stats_by_sector,
    ), None, 200


async def _fetch_delivery_orders_by_sector(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Dict[str, Set[str]]], Optional[str], Optional[int]]:
    skip = 0
    sectors_by_order: Dict[str, Set[str]] = {}

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
            if not isinstance(order_id, str) or not order_id:
                continue
            sector_name = _normalize_sector_name(order.get("sectorName"))
            sectors_by_order.setdefault(sector_name, set()).add(order_id)

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return sectors_by_order, None, 200


async def _fetch_delivery_revenue_by_order(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Dict[str, Decimal]], Optional[str], Optional[int]]:
    skip = 0
    revenue_by_order: Dict[str, Decimal] = {}

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        sales = await dodo_api.get(
            account_id,
            "accounting/sales",
            params=params,
        )
        if not sales.ok:
            return None, sales.error_html, sales.status

        data = sales.data if isinstance(sales.data, dict) else {}
        for sale in data.get("sales", []) or []:
            if not isinstance(sale, dict):
                continue
            if sale.get("salesChannel") != "Delivery":
                continue
            order_id = sale.get("orderId")
            if not isinstance(order_id, str) or not order_id:
                continue
            revenue_by_order[order_id] = revenue_by_order.get(order_id, Decimal("0")) + _sum_sale_revenue(sale)

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return revenue_by_order, None, 200


async def _fetch_stop_seconds_by_sector(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[Optional[Dict[str, int]], Optional[str], Optional[int]]:
    stops = await dodo_api.get(
        account_id,
        "delivery/stop-sales-sectors",
        params=params_base,
    )
    if not stops.ok:
        return None, stops.error_html, stops.status

    stop_seconds_by_sector: Dict[str, int] = {}
    data = stops.data if isinstance(stops.data, dict) else {}
    for stop in data.get("stopSalesBySectors", []) or []:
        if not isinstance(stop, dict):
            continue
        sector_name = _normalize_sector_name(stop.get("sectorName"))
        overlap_seconds = _calculate_stop_overlap_seconds(stop, dt_start=dt_start, dt_end=dt_end)
        if overlap_seconds <= 0:
            continue
        stop_seconds_by_sector[sector_name] = (
            stop_seconds_by_sector.get(sector_name, 0) + overlap_seconds
        )

    return stop_seconds_by_sector, None, 200


def _merge_sector_stats(
    *,
    sectors_by_order: Dict[str, Set[str]],
    revenue_by_order: Dict[str, Decimal],
    stops_by_sector: Dict[str, int],
) -> Dict[str, SectorStats]:
    all_sectors = set(sectors_by_order) | set(stops_by_sector)
    stats_by_sector: Dict[str, SectorStats] = {}

    for sector_name in all_sectors:
        order_ids = sectors_by_order.get(sector_name, set())
        revenue = sum((revenue_by_order.get(order_id, Decimal("0")) for order_id in order_ids), Decimal("0"))
        stats_by_sector[sector_name] = SectorStats(
            orders_count=len(order_ids),
            revenue=revenue,
            stop_seconds=stops_by_sector.get(sector_name, 0),
        )

    return stats_by_sector


def _format_sectors_message(
    *,
    unit_name: str,
    date_label: str,
    stats_by_sector: Dict[str, SectorStats],
) -> str:
    lines = [f"\U0001F5FA <b>Сектора доставки за {date_label} в заведении {unit_name}</b>", ""]

    if not stats_by_sector:
        lines.append("Данных по секторам доставки не найдено.")
        return "\n".join(lines).rstrip()

    sorted_items = sorted(
        stats_by_sector.items(),
        key=lambda item: (-item[1].orders_count, -item[1].revenue, item[0].lower()),
    )
    total_orders = sum(stats.orders_count for stats in stats_by_sector.values())
    total_revenue = sum((stats.revenue for stats in stats_by_sector.values()), Decimal("0"))

    for sector_name, stats in sorted_items:
        orders_percent = _format_share_percent(stats.orders_count, total_orders)
        revenue_percent = _format_share_percent(stats.revenue, total_revenue)
        lines.extend(
            [
                f"<b>{sector_name}</b>",
                f"    Заказы: {stats.orders_count} ({orders_percent})",
                f"    Выручка: {_format_money(stats.revenue)} ({revenue_percent})",
                f"    Стопы: {timedelta(seconds=stats.stop_seconds)}",
            ]
        )

    return "\n".join(lines).rstrip()


def _calculate_stop_overlap_seconds(
    stop: Dict[str, object],
    *,
    dt_start: datetime,
    dt_end: datetime,
) -> int:
    started_at = _parse_dodo_datetime(stop.get("startedAtLocal") or stop.get("startedAt"))
    if started_at is None:
        return 0
    ended_at = _parse_dodo_datetime(stop.get("endedAtLocal") or stop.get("endedAt")) or dt_end

    overlap_start = max(started_at, dt_start)
    overlap_end = min(ended_at, dt_end)
    if overlap_end <= overlap_start:
        return 0
    return int((overlap_end - overlap_start).total_seconds())


def _sum_sale_revenue(sale: Dict[str, object]) -> Decimal:
    total = Decimal("0")
    for product in sale.get("products", []) or []:
        if not isinstance(product, dict):
            continue
        total += _to_decimal(product.get("priceWithDiscount"))
    return total


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


def _normalize_sector_name(value: object) -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return UNKNOWN_SECTOR_NAME


def _format_money(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.01"))
    if normalized == normalized.quantize(Decimal("1")):
        return str(int(normalized))
    return f"{normalized:.2f}".replace(".", ",")


def _format_share_percent(value: int | Decimal, total: int | Decimal) -> str:
    if not total:
        return "0%"
    percent = round((Decimal(value) / Decimal(total)) * Decimal("100"))
    return f"{int(percent)}%"


def _to_decimal(value: object) -> Decimal:
    try:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value or value.startswith("0001-01-01"):
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

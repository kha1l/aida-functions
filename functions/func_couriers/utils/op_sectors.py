"""Сбор manual-отчета по секторам доставки."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Set

from dagster import OpExecutionContext

from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    parse_dodo_datetime,
    run_manual_courier_report,
)

PAGE_SIZE = 500
UNKNOWN_SECTOR_NAME = "Без сектора"


@dataclass(frozen=True, slots=True)
class SectorStats:
    orders_count: int
    revenue: Decimal
    stop_seconds: int


async def run_sectors(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по секторам доставки для одной точки."""
    await run_manual_courier_report(
        context,
        post="sectors",
        log_prefix="sectors_op",
        build_report=_build_sectors_report,
    )


async def _build_sectors_report(
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

    sectors_by_order_task = _fetch_delivery_orders_by_sector(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
    )
    revenue_by_order_task = _fetch_delivery_revenue_by_order(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
    )
    stops_by_sector_task = _fetch_stop_seconds_by_sector(
        dodo_api=dodo_api,
        account_id=request.account_id,
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
        unit_name=request.unit_name,
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
    started_at = parse_dodo_datetime(stop.get("startedAtLocal") or stop.get("startedAt"))
    if started_at is None:
        return 0
    ended_at = parse_dodo_datetime(stop.get("endedAtLocal") or stop.get("endedAt")) or dt_end

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

"""Ежедневный job по ресторану: продажи Dine-in, handover-метрики и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set

import aiohttp
from dagster import OpExecutionContext
from functions.shared.manual_report_runtime import (
    html_to_log_text,
    split_text_for_telegram,
    to_int,
)

from database.requests import (
    ReportSubscription,
    load_accounts_and_tokens_bulk,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_report_subscriptions,
)
from messaging import build_payload

PAGE_SIZE = 500
MAX_CONCURRENT_UNITS = 15


@dataclass(frozen=True, slots=True)
class StationaryHandoverStats:
    """Агрегаты по заказам ресторана из orders-handover-time."""

    count_later: int
    dine_in_orders_count: int
    total_prep_seconds: int


@dataclass(frozen=True, slots=True)
class StationarySalesStats:
    """Агрегаты по продажам ресторана из accounting/sales."""

    revenue_total: Decimal
    orders_total: int
    mobile_orders_total: int
    kiosk_orders_total: int
    tray_orders_total: int
    combo_revenue_total: Decimal
    added_ingredients_revenue_total: Decimal


async def run_stationary(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных stationary-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    subscriptions = await select_report_subscriptions(db, "stationary")
    if not subscriptions:
        context.log.info("stationary_op: нет подписок")
        return

    # Дальше job работает по снимку подписок и candidate account'ов, без дополнительных запросов на точку.
    subs_by_unit: Dict[str, List[ReportSubscription]] = {}
    accounts_by_unit: Dict[str, List[int]] = {}
    for sub in subscriptions:
        subs_by_unit.setdefault(sub.unit_uuid, []).append(sub)
        if sub.account_id is not None:
            accounts_by_unit.setdefault(sub.unit_uuid, []).append(sub.account_id)

    unit_uuids = list(subs_by_unit.keys())
    units, candidate_accounts_by_unit = await asyncio.gather(
        load_units_by_uuids(db, unit_uuids),
        select_account_ids_for_units(
            db,
            unit_uuids,
            account_ids_filter_by_unit=accounts_by_unit,
        ),
    )

    all_account_ids = {
        account_id
        for account_ids in accounts_by_unit.values()
        for account_id in account_ids
    }
    all_account_ids.update(
        account_id
        for account_ids in candidate_accounts_by_unit.values()
        for account_id in account_ids
    )
    account_auth_map = await load_accounts_and_tokens_bulk(db, all_account_ids)
    dodo_api.prime_account_token_cache(account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))

    timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as dodo_session:
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=request_semaphore,
        )
        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                subscribers = subs_by_unit.get(unit_uuid, [])
                unit = units.get(unit_uuid)
                unit_name = unit.name if unit and unit.name else unit_uuid

                if not subscribers:
                    return

                order_account_ids = list({acc for acc in accounts_by_unit.get(unit_uuid, [])})
                candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, []) or order_account_ids
                if not candidate_accounts:
                    context.log.warning("stationary_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None

                for account_id in candidate_accounts:
                    # Фолбэк по токенам такой же, как в couriers: пока не получим первый успешный ответ.
                    report_text, last_error_html, last_error_status = await _build_stationary_report(
                        dodo_api=dodo_api,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                    )
                    if report_text:
                        break
                    if last_error_html and last_error_status in {401, 403}:
                        context.log.warning(
                            "stationary_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            html_to_log_text(last_error_html),
                        )

                if not report_text:
                    if last_error_html:
                        if last_error_status in {429, 500}:
                            context.log.error(
                                "stationary_op: не удалось собрать отчет для unit %s после retry: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                        elif last_error_status not in {401, 403}:
                            context.log.error(
                                "stationary_op: не удалось собрать отчет для unit %s: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                    else:
                        context.log.warning("stationary_op: нет данных для unit %s", unit_uuid)
                    return

                for text_chunk in split_text_for_telegram(report_text):
                    for sub in subscribers:
                        payload = build_payload(
                            chat_id=sub.chat_id,
                            topic_id=sub.topic_id,
                            post="stationary",
                            text=text_chunk,
                            unit_uuid=unit_uuid,
                        )
                        async with publish_lock:
                            await rabbitmq.publish_async(payload)

        try:
            await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def _build_stationary_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(days=1)
    date_label = dt_start.strftime("%d.%m")

    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    handover_task = dodo_api.get(
        account_id,
        "production/orders-handover-time",
        params=params_base,
    )
    sales_stats_task = _fetch_stationary_sales_stats(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_base,
    )
    handover, sales_stats_result = await asyncio.gather(
        handover_task,
        sales_stats_task,
    )

    if not handover.ok:
        return None, handover.error_html, handover.status
    if sales_stats_result[0] is None:
        return None, sales_stats_result[1], sales_stats_result[2]

    handover_stats = _extract_handover_stats(
        handover.data if isinstance(handover.data, dict) else {}
    )
    sales_stats = sales_stats_result[0]
    return _format_stationary_message(
        unit_name=unit_name,
        date_label=date_label,
        handover_stats=handover_stats,
        sales_stats=sales_stats,
    ), None, 200


async def _fetch_stationary_sales_stats(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[StationarySalesStats], Optional[str], Optional[int]]:
    """Суммирует все ключевые stationary-метрики из accounting/sales за один проход."""
    skip = 0
    order_ids: Set[str] = set()
    mobile_order_ids: Set[str] = set()
    kiosk_order_ids: Set[str] = set()
    tray_order_ids: Set[str] = set()
    revenue_total = Decimal("0")
    combo_revenue_total = Decimal("0")
    added_ingredients_revenue_total = Decimal("0")

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
            if sale.get("salesChannel") != "Dine-in":
                continue

            order_key = _build_order_key(sale.get("orderId"), sale.get("orderNumber"))
            if not order_key:
                continue

            order_ids.add(order_key)

            # Проценты по каналам считаем по уникальным заказам, а не по строкам продуктов.
            order_source = str(sale.get("orderSource") or "").strip()
            if order_source == "MobileApp":
                mobile_order_ids.add(order_key)
            if order_source == "Kiosk":
                kiosk_order_ids.add(order_key)
            if order_source == "Dine-in":
                tray_order_ids.add(order_key)

            for product in sale.get("products", []) or []:
                if not isinstance(product, dict):
                    continue
                revenue_total += _to_decimal(product.get("priceWithDiscount"))
                if product.get("combo") is not None:
                    combo_revenue_total += _to_decimal(product.get("priceWithDiscount"))

                for ingredient in product.get("addedIngredients", []) or []:
                    if not isinstance(ingredient, dict):
                        continue
                    added_ingredients_revenue_total += _to_decimal(ingredient.get("price"))

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return (
        StationarySalesStats(
            revenue_total=revenue_total,
            orders_total=len(order_ids),
            mobile_orders_total=len(mobile_order_ids),
            kiosk_orders_total=len(kiosk_order_ids),
            tray_orders_total=len(tray_order_ids),
            combo_revenue_total=combo_revenue_total,
            added_ingredients_revenue_total=added_ingredients_revenue_total,
        ),
        None,
        200,
    )


def _extract_handover_stats(data: Dict[str, object]) -> StationaryHandoverStats:
    """Считает среднее время приготовления и долю долгих заказов ресторана."""
    count_later = 0
    dine_in_orders_count = 0
    total_prep_seconds = 0

    for order in data.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Dine-in":
            continue

        tracking_pending = to_int(order.get("trackingPendingTime")) or 0
        cooking_time = to_int(order.get("cookingTime")) or 0
        meeting_time = tracking_pending + cooking_time

        total_prep_seconds += meeting_time
        dine_in_orders_count += 1

        if meeting_time > _late_threshold_seconds(order.get("orderNumber")):
            count_later += 1

    return StationaryHandoverStats(
        count_later=count_later,
        dine_in_orders_count=dine_in_orders_count,
        total_prep_seconds=total_prep_seconds,
    )


def _format_stationary_message(
    *,
    unit_name: str,
    date_label: str,
    handover_stats: StationaryHandoverStats,
    sales_stats: StationarySalesStats,
) -> str:
    avg_prep = timedelta(
        seconds=round(handover_stats.total_prep_seconds / handover_stats.dine_in_orders_count)
    ) if handover_stats.dine_in_orders_count else timedelta(0)
    long_orders_percent = _format_percent(
        _share_percent(handover_stats.count_later, handover_stats.dine_in_orders_count)
    )
    mobile_percent = _format_share_percent(
        sales_stats.mobile_orders_total,
        sales_stats.orders_total,
    )
    kiosk_percent = _format_share_percent(
        sales_stats.kiosk_orders_total,
        sales_stats.orders_total,
    )
    tray_percent = _format_share_percent(
        sales_stats.tray_orders_total,
        sales_stats.orders_total,
    )
    combo_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.combo_revenue_total,
            sales_stats.revenue_total,
        )
    )
    added_ingredients_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.added_ingredients_revenue_total,
            sales_stats.revenue_total,
        )
    )

    lines = [
        f"\U0001F307 Краткий отчет по ресторану за {date_label}",
        "",
        f"\U0001F373 <b>{unit_name}</b>",
        f"Выручка: {_format_money(sales_stats.revenue_total)}",
        f"Заказы: {sales_stats.orders_total}",
        f"Среднее время приготовления: {avg_prep}",
        f"Долгие заказы: {long_orders_percent}%",
        f"Заказов через приложение: {sales_stats.mobile_orders_total} ({mobile_percent})",
        f"Заказов через киоск: {sales_stats.kiosk_orders_total} ({kiosk_percent})",
        f"Заказов на подносе: {sales_stats.tray_orders_total} ({tray_percent})",
        f"Продаж комбо: {_format_money(sales_stats.combo_revenue_total)} ({combo_revenue_percent}%)",
        (
            "Продаж доп. ингредиентов: "
            f"{_format_money(sales_stats.added_ingredients_revenue_total)} "
            f"({added_ingredients_revenue_percent}%)"
        ),
        "",
        "#отчетПоРесторану",
    ]
    return "\n".join(lines).rstrip()


def _late_threshold_seconds(order_number: object) -> int:
    parts_count = _extract_order_parts_count(order_number)
    if parts_count <= 0:
        return 300
    if parts_count < 4:
        return 900
    return 1500


def _extract_order_parts_count(order_number: object) -> int:
    if not isinstance(order_number, str) or "-" not in order_number:
        return 0
    try:
        return int(order_number.rsplit("-", 1)[-1])
    except ValueError:
        return 0


def _build_order_key(order_id: object, order_number: object) -> Optional[str]:
    if isinstance(order_id, str) and order_id:
        return order_id
    if isinstance(order_number, str) and order_number:
        return order_number
    return None


def _format_money(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.01"))
    if normalized == normalized.quantize(Decimal("1")):
        return str(int(normalized))
    return f"{normalized:.2f}".replace(".", ",")


def _format_share_percent(value: int, total: int) -> str:
    if not total:
        return "0%"
    return f"{int(round((value / total) * 100, 0))}%"


def _share_percent(value: int, total: int) -> float:
    if not total:
        return 0.0
    return round((value / total) * 100, 2)


def _share_decimal_percent(value: Decimal, total: Decimal) -> float:
    if not total:
        return 0.0
    try:
        return round(float((value / total) * Decimal("100")), 2)
    except (InvalidOperation, ZeroDivisionError):
        return 0.0


def _format_percent(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _to_decimal(value: object) -> Decimal:
    try:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

"""Ежедневный job по ресторану: продажи Dine-in, handover-метрики и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set

from dagster import OpExecutionContext
from functions.shared.manual_report_runtime import to_int
from config import schedule_local_hours
from functions.shared.daily_job_config import (
    configured_report_date,
    is_configured_run,
    select_configured_unit_uuids,
)
from functions.shared.customer_ratings import (
    CustomerRatings,
    fetch_customer_ratings,
    format_customer_rating,
)
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)

from database.requests import select_report_subscriptions
from functions.shared.unit_time import build_report_local_day_window, resolve_unit_now

PAGE_SIZE = 500
MAX_CONCURRENT_UNITS = 15
TRIGGER_HOURS = set(schedule_local_hours("stationary", (10,)))


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
    dine_in_combo_revenue_total: Decimal
    dine_in_added_ingredients_revenue_total: Decimal
    kiosk_combo_revenue_total: Decimal
    kiosk_added_ingredients_revenue_total: Decimal


async def run_stationary(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных stationary-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "stationary"),
    )
    if not snapshot.subscriptions:
        context.log.info("stationary_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    configured_run = is_configured_run(context)
    report_date = configured_report_date(context)
    if configured_run:
        unit_uuids = select_configured_unit_uuids(
            context,
            available_unit_uuids=snapshot.subs_by_unit.keys(),
            log_prefix="stationary_op",
        )
    else:
        unit_uuids = [
            unit_uuid
            for unit_uuid in snapshot.subs_by_unit.keys()
            if resolve_unit_now(snapshot.unit_for(unit_uuid)).hour in TRIGGER_HOURS
        ]
    if not unit_uuids:
        if configured_run:
            context.log.info("stationary_op: нет точек для заданных параметров запуска")
        else:
            context.log.info(
                "stationary_op: нет точек на локальные часы из SCHEDULE_STATIONARY_HOURS",
            )
        return

    async with report_job_runtime(dodo_api, rabbitmq):
        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                subscribers = snapshot.subscribers_for(unit_uuid)
                unit = snapshot.unit_for(unit_uuid)
                unit_name = snapshot.unit_name_for(unit_uuid)

                if not subscribers:
                    return

                candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
                if not candidate_accounts:
                    context.log.warning("stationary_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_stationary_report(
                        dodo_api=dodo_api,
                        unit=unit,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                        report_date=report_date,
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="stationary_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="stationary_op",
                        unit_uuid=unit_uuid,
                        error_html=last_error_html,
                        error_status=last_error_status,
                        empty_message="нет данных",
                    )
                    return

                if not report_text:
                    return

                await publish_report_to_subscribers(
                    rabbitmq,
                    publish_lock=publish_lock,
                    subscribers=subscribers,
                    post="stationary",
                    text=report_text,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def _build_stationary_report(
    *,
    dodo_api,
    unit: Optional[object],
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    report_date: Optional[object] = None,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end = build_report_local_day_window(unit, report_date)
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
    ratings_task = fetch_customer_ratings(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        report_day=dt_start.date(),
    )
    handover, sales_stats_result, ratings_result = await asyncio.gather(
        handover_task,
        sales_stats_task,
        ratings_task,
    )

    if not handover.ok:
        return None, handover.error_html, handover.status
    if sales_stats_result[0] is None:
        return None, sales_stats_result[1], sales_stats_result[2]
    if ratings_result[0] is None:
        return None, ratings_result[1], ratings_result[2]

    handover_stats = _extract_handover_stats(
        handover.data if isinstance(handover.data, dict) else {}
    )
    sales_stats = sales_stats_result[0]
    ratings = ratings_result[0]
    assert ratings is not None
    return _format_stationary_message(
        unit_name=unit_name,
        date_label=date_label,
        handover_stats=handover_stats,
        sales_stats=sales_stats,
        ratings=ratings,
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
    dine_in_combo_revenue_total = Decimal("0")
    dine_in_added_ingredients_revenue_total = Decimal("0")
    kiosk_combo_revenue_total = Decimal("0")
    kiosk_added_ingredients_revenue_total = Decimal("0")

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
            order_key = _build_order_key(sale.get("orderId"), sale.get("orderNumber"))
            if not order_key:
                continue

            sales_channel = str(sale.get("salesChannel") or "").strip()
            if sales_channel == "Dine-in":
                order_ids.add(order_key)

            # Проценты по каналам считаем по уникальным заказам, а не по строкам продуктов.
            order_source = str(sale.get("orderSource") or "").strip()
            if order_source == "MobileApp" and sales_channel == "Dine-in":
                mobile_order_ids.add(order_key)
            if order_source == "Kiosk":
                kiosk_order_ids.add(order_key)
            if order_source == "Dine-in":
                tray_order_ids.add(order_key)

            for product in sale.get("products", []) or []:
                if not isinstance(product, dict):
                    continue
                product_price = _to_decimal(product.get("priceWithDiscount"))
                if sales_channel == "Dine-in":
                    revenue_total += product_price
                if order_source == "Dine-in":
                    if product.get("combo") is not None:
                        dine_in_combo_revenue_total += product_price
                elif order_source == "Kiosk" and product.get("combo") is not None:
                    kiosk_combo_revenue_total += product_price

                for ingredient in product.get("addedIngredients", []) or []:
                    if not isinstance(ingredient, dict):
                        continue
                    ingredient_price = _to_decimal(ingredient.get("price"))
                    if order_source == "Dine-in":
                        dine_in_added_ingredients_revenue_total += ingredient_price
                    elif order_source == "Kiosk":
                        kiosk_added_ingredients_revenue_total += ingredient_price

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
            dine_in_combo_revenue_total=dine_in_combo_revenue_total,
            dine_in_added_ingredients_revenue_total=dine_in_added_ingredients_revenue_total,
            kiosk_combo_revenue_total=kiosk_combo_revenue_total,
            kiosk_added_ingredients_revenue_total=kiosk_added_ingredients_revenue_total,
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
    ratings: CustomerRatings,
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
    dine_in_combo_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.dine_in_combo_revenue_total,
            sales_stats.revenue_total,
        )
    )
    dine_in_added_ingredients_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.dine_in_added_ingredients_revenue_total,
            sales_stats.revenue_total,
        )
    )
    kiosk_combo_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.kiosk_combo_revenue_total,
            sales_stats.revenue_total,
        )
    )
    kiosk_added_ingredients_revenue_percent = _format_percent(
        _share_decimal_percent(
            sales_stats.kiosk_added_ingredients_revenue_total,
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
        f"Рейтинг клиента в ресторане: {format_customer_rating(ratings.avg_dine_in_order_rate, ratings.dine_in_rate_count)}",
        f"Заказов через приложение: {sales_stats.mobile_orders_total} ({mobile_percent})",
        f"Заказов через киоск: {sales_stats.kiosk_orders_total} ({kiosk_percent})",
        "Продажи через ресторан:",
        (
            f"     Комбо: {_format_money(sales_stats.dine_in_combo_revenue_total)} "
            f"({dine_in_combo_revenue_percent}%)"
        ),
        (
            "     Доп. ингредиенты: "
            f"{_format_money(sales_stats.dine_in_added_ingredients_revenue_total)} "
            f"({dine_in_added_ingredients_revenue_percent}%)"
        ),
        "Продажи через киоск:",
        (
            f"     Комбо: {_format_money(sales_stats.kiosk_combo_revenue_total)} "
            f"({kiosk_combo_revenue_percent}%)"
        ),
        (
            "     Доп. ингредиенты: "
            f"{_format_money(sales_stats.kiosk_added_ingredients_revenue_total)} "
            f"({kiosk_added_ingredients_revenue_percent}%)"
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

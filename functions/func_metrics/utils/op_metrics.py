"""Почасовой job по ключевым метрикам с локальными часами из env."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.requests import select_report_subscriptions
from database.requests import (
    load_accounts_and_tokens_bulk,
    load_chat_languages,
    load_units_by_uuids,
    select_account_ids_for_units,
)
from functions.shared.manual_report_runtime import to_int
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.report_text_i18n import translate_report_text
from functions.shared.unit_time import floor_unit_to_hour, resolve_unit_now
from functions.func_couriers.utils.manual_report_common import parse_dodo_datetime
from messaging import build_payload

TRIGGER_HOURS = set(schedule_local_hours("metrics", (14, 19, 23)))
MAX_CONCURRENT_UNITS = 15
PAGE_SIZE = 500
COMPARE_PERIOD_DAYS = 7
LONG_DINE_IN_THRESHOLD_SECONDS = 15 * 60


@dataclass(frozen=True, slots=True)
class MetricsSalesTotals:
    """Агрегаты продаж за период по всем каналам."""

    revenue_total: Decimal
    orders_total: int


@dataclass(frozen=True, slots=True)
class MetricsDeliveryStats:
    """Ключевые метрики доставки из `delivery/statistics`."""

    avg_delivery_seconds: int
    avg_cooking_seconds: int
    avg_heated_shelf_seconds: int
    avg_trip_seconds: int
    late_orders_count: int


@dataclass(frozen=True, slots=True)
class MetricsProductivityStats:
    """Ключевые метрики производительности из `production/productivity`."""

    sales_per_labor_hour: float
    products_per_labor_hour: float
    orders_per_courier_labor_hour: float


@dataclass(frozen=True, slots=True)
class MetricsHandoverStats:
    """Агрегаты по заказам ресторана и доставки из `orders-handover-time`."""

    dine_in_orders_count: int
    dine_in_total_meeting_seconds: int
    dine_in_long_orders_count: int
    delivery_orders_count: int
    delivery_total_cooking_seconds: int


@dataclass(frozen=True, slots=True)
class MetricsCustomerRatings:
    """Средние оценки и количество оцененных заказов по каналам."""

    avg_dine_in_order_rate: float
    avg_delivery_order_rate: float
    dine_in_rate_count: int
    delivery_rate_count: int


async def run_metrics(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ключевых метрик по всем подпискам metrics."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "metrics"),
    )
    if not snapshot.subscriptions:
        context.log.info("metrics_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    unit_uuids = list(snapshot.subs_by_unit.keys())

    async with report_job_runtime(dodo_api, rabbitmq):
        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                subscribers = snapshot.subscribers_for(unit_uuid)
                unit = snapshot.unit_for(unit_uuid)
                unit_name = snapshot.unit_name_for(unit_uuid)

                if not subscribers:
                    return

                local_now = resolve_unit_now(unit)
                if local_now.hour not in TRIGGER_HOURS:
                    return

                # `production/productivity` принимает только часы, поэтому срезаем период до текущего часа.
                period_end = floor_unit_to_hour(unit)
                if period_end.hour not in TRIGGER_HOURS:
                    return

                candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
                if not candidate_accounts:
                    context.log.warning("metrics_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    account = snapshot.account_for(account_id)
                    report_text, last_error_html, last_error_status = await _build_metrics_report(
                        dodo_api=dodo_api,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                        period_end=period_end,
                        market_code=_extract_market_code(
                            account.properties if account else None
                        ),
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="metrics_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="metrics_op",
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
                    post="metrics",
                    text=report_text,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def run_metrics_manual(context: OpExecutionContext) -> None:
    """Собирает оперативный отчет по метрикам для выбранных пользователем заведений."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    unit_uuids = _normalize_unit_uuids(context.op_config.get("unit_uuids"))
    if not unit_uuids:
        context.log.warning("metrics_manual_op: не переданы unit_uuids")
        return

    units = await load_units_by_uuids(db, unit_uuids)
    candidate_accounts_by_unit = await select_account_ids_for_units(db, unit_uuids)
    all_account_ids = {
        account_id
        for account_ids in candidate_accounts_by_unit.values()
        for account_id in account_ids
    }
    account_auth_map = await load_accounts_and_tokens_bulk(db, all_account_ids)
    dodo_api.prime_account_token_cache(account_auth_map)

    chat_id = int(context.op_config["chat_id"])
    topic_id = context.op_config.get("topic_id")
    fallback_account_id = next(iter(sorted(all_account_ids)), None)
    chat_languages = await load_chat_languages(
        db,
        [chat_id],
        fallback_account_ids_by_chat={chat_id: fallback_account_id} if fallback_account_id is not None else None,
    )
    report_language = chat_languages.get(chat_id, "ru")

    async with report_job_runtime(dodo_api, rabbitmq):
        sorted_unit_uuids = sorted(
            unit_uuids,
            key=lambda unit_uuid: (
                units.get(unit_uuid).name.lower()
                if units.get(unit_uuid) and units.get(unit_uuid).name
                else unit_uuid
            ),
        )

        for unit_uuid in sorted_unit_uuids:
            unit = units.get(unit_uuid)
            unit_name = unit.name if unit and unit.name else unit_uuid
            period_end = floor_unit_to_hour(unit)
            candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, [])
            if not candidate_accounts:
                context.log.warning("metrics_manual_op: нет токенов для unit %s", unit_uuid)
                continue

            report_text: Optional[str] = None
            last_error_html: Optional[str] = None
            last_error_status: Optional[int] = None

            for account_id in candidate_accounts:
                account, _token = account_auth_map.get(account_id, (None, None))
                report_text, last_error_html, last_error_status = await _build_metrics_report(
                    dodo_api=dodo_api,
                    unit_uuid=unit_uuid,
                    unit_name=unit_name,
                    account_id=account_id,
                    period_end=period_end,
                    market_code=_extract_market_code(
                        account.properties if account else None
                    ),
                )
                if last_error_html is None and last_error_status == 200:
                    break
                log_auth_error(
                    context,
                    log_prefix="metrics_manual_op",
                    unit_uuid=unit_uuid,
                    account_id=account_id,
                    error_html=last_error_html,
                    error_status=last_error_status,
                )

            if not report_text:
                log_final_unit_error(
                    context,
                    log_prefix="metrics_manual_op",
                    unit_uuid=unit_uuid,
                    error_html=last_error_html,
                    error_status=last_error_status,
                    empty_message="нет данных",
                )
                continue

            payload = build_payload(
                chat_id=chat_id,
                topic_id=topic_id,
                post="metrics",
                text=translate_report_text("metrics", report_language, report_text),
                unit_uuid=unit_uuid,
            )
            await rabbitmq.publish_async(payload)


async def _build_metrics_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    period_end: datetime,
    market_code: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Собирает единый текст отчета по ключевым метрикам для одной точки."""
    current_start = period_end.replace(hour=0, minute=0, second=0, microsecond=0)
    previous_start = current_start - timedelta(days=COMPARE_PERIOD_DAYS)
    previous_end = period_end - timedelta(days=COMPARE_PERIOD_DAYS)

    params_current = {
        "units": unit_uuid,
        "from": current_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": period_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    params_previous = {
        "units": unit_uuid,
        "from": previous_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": previous_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    sales_current_task = _fetch_sales_totals(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_current,
    )
    sales_previous_task = _fetch_sales_totals(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_previous,
    )
    delivery_task = dodo_api.get(
        account_id,
        "delivery/statistics/",
        params=params_current,
    )
    productivity_task = dodo_api.get(
        account_id,
        "production/productivity",
        params=params_current,
    )
    courier_orders_task = _fetch_delivery_times_by_order_id(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=params_current,
    )
    handover_task = dodo_api.get(
        account_id,
        "production/orders-handover-time",
        params=params_current,
    )
    ratings_task = _fetch_customer_ratings(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        report_day=current_start.date() - timedelta(days=1),
    )
    couriers_on_shift_task = _count_staff_on_shift(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        current_start=current_start,
        snapshot_at=period_end,
        staff_type_name="Courier",
    )
    kitchen_on_shift_task = _count_staff_on_shift(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        current_start=current_start,
        snapshot_at=period_end,
        staff_type_name="KitchenMember",
    )

    (
        current_sales_result,
        previous_sales_result,
        delivery_result,
        productivity_result,
        courier_orders_result,
        handover_result,
        ratings_result,
        couriers_on_shift_result,
        kitchen_on_shift_result,
    ) = await asyncio.gather(
        sales_current_task,
        sales_previous_task,
        delivery_task,
        productivity_task,
        courier_orders_task,
        handover_task,
        ratings_task,
        couriers_on_shift_task,
        kitchen_on_shift_task,
    )

    for result in (current_sales_result, previous_sales_result):
        if result[0] is None:
            return None, result[1], result[2]

    if not delivery_result.ok:
        return None, delivery_result.error_html, delivery_result.status
    if not productivity_result.ok:
        return None, productivity_result.error_html, productivity_result.status
    if courier_orders_result[0] is None:
        return None, courier_orders_result[1], courier_orders_result[2]
    if not handover_result.ok:
        return None, handover_result.error_html, handover_result.status
    if ratings_result[0] is None:
        return None, ratings_result[1], ratings_result[2]
    if couriers_on_shift_result[0] is None:
        return None, couriers_on_shift_result[1], couriers_on_shift_result[2]
    if kitchen_on_shift_result[0] is None:
        return None, kitchen_on_shift_result[1], kitchen_on_shift_result[2]

    current_sales = current_sales_result[0]
    previous_sales = previous_sales_result[0]
    assert current_sales is not None
    assert previous_sales is not None

    delivery_stats = _extract_delivery_stats(
        delivery_result.data if isinstance(delivery_result.data, dict) else {}
    )
    productivity_stats = _extract_productivity_stats(
        productivity_result.data if isinstance(productivity_result.data, dict) else {}
    )
    delivery_times_by_order_id = courier_orders_result[0] or {}
    handover_stats = _extract_handover_stats(
        handover_result.data if isinstance(handover_result.data, dict) else {}
    )
    customer_ratings = ratings_result[0]
    assert customer_ratings is not None
    long_delivery_percent = _calculate_long_deliveries_percent(
        statistics=delivery_result.data if isinstance(delivery_result.data, dict) else {},
        delivery_times_by_order_id=delivery_times_by_order_id,
        handover=handover_result.data if isinstance(handover_result.data, dict) else {},
        market_code=market_code,
    )

    return (
        _format_metrics_message(
            unit_name=unit_name,
            current_revenue=current_sales.revenue_total,
            previous_revenue=previous_sales.revenue_total,
            productivity_stats=productivity_stats,
            delivery_stats=delivery_stats,
            handover_stats=handover_stats,
            customer_ratings=customer_ratings,
            kitchen_on_shift=kitchen_on_shift_result[0] or 0,
            couriers_on_shift=couriers_on_shift_result[0] or 0,
            long_delivery_percent=long_delivery_percent,
        ),
        None,
        200,
    )


async def _fetch_sales_totals(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[MetricsSalesTotals], Optional[str], Optional[int]]:
    """Считает выручку и количество заказов по `accounting/sales` за период."""
    skip = 0
    revenue_total = Decimal("0")
    order_ids: Set[str] = set()

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        result = await dodo_api.get(
            account_id,
            "accounting/sales",
            params=params,
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data if isinstance(result.data, dict) else {}
        for sale in data.get("sales", []) or []:
            if not isinstance(sale, dict):
                continue

            order_key = _build_order_key(sale.get("orderId"), sale.get("orderNumber"))
            if order_key:
                order_ids.add(order_key)

            for product in sale.get("products", []) or []:
                if not isinstance(product, dict):
                    continue
                revenue_total += _to_decimal(product.get("priceWithDiscount"))

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return MetricsSalesTotals(
        revenue_total=revenue_total,
        orders_total=len(order_ids),
    ), None, 200


async def _fetch_delivery_times_by_order_id(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Dict[str, int]], Optional[str], Optional[int]]:
    """Выгружает доставочные заказы и сохраняет deliveryTime по orderId."""
    skip = 0
    delivery_times_by_order_id: Dict[str, int] = {}

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        result = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params=params,
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data if isinstance(result.data, dict) else {}
        for order in data.get("couriersOrders", []) or []:
            if not isinstance(order, dict):
                continue
            order_id = order.get("orderId")
            delivery_time = to_int(order.get("deliveryTime"))
            if isinstance(order_id, str) and order_id and delivery_time is not None:
                delivery_times_by_order_id[order_id] = delivery_time

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return delivery_times_by_order_id, None, 200


async def _fetch_customer_ratings(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    report_day: date,
) -> tuple[Optional[MetricsCustomerRatings], Optional[str], Optional[int]]:
    """Получает средние оценки заказов по ресторану и доставке за последний доступный день."""
    result = await dodo_api.get(
        account_id,
        "https://api.dodois.io/customer-feedback/customer-ratings",
        params={
            "units": unit_uuid,
            "from": report_day.isoformat(),
            "to": report_day.isoformat(),
        },
    )
    if not result.ok:
        return None, result.error_html, result.status

    data = result.data if isinstance(result.data, dict) else {}
    rows = data.get("customerRatings", []) or []
    row = rows[0] if rows else {}
    if not isinstance(row, dict):
        row = {}

    return MetricsCustomerRatings(
        avg_dine_in_order_rate=_to_float(row.get("avgDineInOrderRate")),
        avg_delivery_order_rate=_to_float(row.get("avgDeliveryOrderRate")),
        dine_in_rate_count=to_int(row.get("dineInRateCount")) or 0,
        delivery_rate_count=to_int(row.get("deliveryRateCount")) or 0,
    ), None, 200


async def _count_staff_on_shift(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    current_start: datetime,
    snapshot_at: datetime,
    staff_type_name: str,
) -> tuple[Optional[int], Optional[str], Optional[int]]:
    """Считает сотрудников нужного типа, которые фактически были на смене в момент snapshot_at."""
    skip = 0
    active_staff_ids: Set[str] = set()

    while True:
        result = await dodo_api.get(
            account_id,
            "staff/shifts",
            params={
                "units": unit_uuid,
                "clockInFrom": current_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "clockInTo": snapshot_at.strftime("%Y-%m-%dT%H:%M:%S"),
                "staffTypeName": staff_type_name,
                "skip": skip,
                "take": PAGE_SIZE,
            },
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data if isinstance(result.data, dict) else {}
        for shift in data.get("shifts", []) or []:
            if not isinstance(shift, dict):
                continue
            staff_id = shift.get("staffId")
            clock_in_at = parse_dodo_datetime(shift.get("clockInAtLocal"))
            clock_out_at = parse_dodo_datetime(shift.get("clockOutAtLocal"))
            if not isinstance(staff_id, str) or not staff_id or clock_in_at is None:
                continue
            if clock_in_at <= snapshot_at and (clock_out_at is None or clock_out_at >= snapshot_at):
                active_staff_ids.add(staff_id)

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return len(active_staff_ids), None, 200


def _extract_delivery_stats(data: Dict[str, object]) -> MetricsDeliveryStats:
    """Извлекает основные delivery-метрики из первого элемента `unitsStatistics`."""
    units_statistics = data.get("unitsStatistics", []) or []
    stats = units_statistics[0] if units_statistics else {}
    if not isinstance(stats, dict):
        stats = {}

    return MetricsDeliveryStats(
        avg_delivery_seconds=to_int(stats.get("avgDeliveryOrderFulfillmentTime")) or 0,
        avg_cooking_seconds=to_int(stats.get("avgCookingTime")) or 0,
        avg_heated_shelf_seconds=to_int(stats.get("avgHeatedShelfTime")) or 0,
        avg_trip_seconds=to_int(stats.get("avgOrderTripTime")) or 0,
        late_orders_count=to_int(stats.get("lateOrdersCount")) or 0,
    )


def _extract_productivity_stats(data: Dict[str, object]) -> MetricsProductivityStats:
    """Извлекает показатели производительности из первого элемента `productivityStatistics`."""
    productivity_statistics = data.get("productivityStatistics", []) or []
    stats = productivity_statistics[0] if productivity_statistics else {}
    if not isinstance(stats, dict):
        stats = {}

    return MetricsProductivityStats(
        sales_per_labor_hour=_to_float(stats.get("salesPerLaborHour")),
        products_per_labor_hour=_to_float(stats.get("productsPerLaborHour")),
        orders_per_courier_labor_hour=_to_float(stats.get("ordersPerCourierLabourHour")),
    )


def _extract_handover_stats(data: Dict[str, object]) -> MetricsHandoverStats:
    """Считает среднее время приготовления в ресторане и доставке по детальным заказам."""
    dine_in_orders_count = 0
    dine_in_total_meeting_seconds = 0
    dine_in_long_orders_count = 0
    delivery_orders_count = 0
    delivery_total_cooking_seconds = 0

    for order in data.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue

        sales_channel = str(order.get("salesChannel") or "")
        tracking_pending = to_int(order.get("trackingPendingTime")) or 0
        cooking_time = to_int(order.get("cookingTime")) or 0

        if sales_channel == "Dine-in":
            meeting_time = tracking_pending + cooking_time
            dine_in_orders_count += 1
            dine_in_total_meeting_seconds += meeting_time
            if meeting_time > LONG_DINE_IN_THRESHOLD_SECONDS:
                dine_in_long_orders_count += 1
        elif sales_channel == "Delivery":
            delivery_orders_count += 1
            delivery_total_cooking_seconds += cooking_time

    return MetricsHandoverStats(
        dine_in_orders_count=dine_in_orders_count,
        dine_in_total_meeting_seconds=dine_in_total_meeting_seconds,
        dine_in_long_orders_count=dine_in_long_orders_count,
        delivery_orders_count=delivery_orders_count,
        delivery_total_cooking_seconds=delivery_total_cooking_seconds,
    )


def _format_metrics_message(
    *,
    unit_name: str,
    current_revenue: Decimal,
    previous_revenue: Decimal,
    productivity_stats: MetricsProductivityStats,
    delivery_stats: MetricsDeliveryStats,
    handover_stats: MetricsHandoverStats,
    customer_ratings: MetricsCustomerRatings,
    kitchen_on_shift: int,
    couriers_on_shift: int,
    long_delivery_percent: float,
) -> str:
    """Формирует итоговый текст сообщения в стиле старого `metrics.py`."""
    avg_rest_prep = _average_timedelta(
        handover_stats.dine_in_total_meeting_seconds,
        handover_stats.dine_in_orders_count,
    )
    avg_delivery_cooking = _average_timedelta(
        handover_stats.delivery_total_cooking_seconds,
        handover_stats.delivery_orders_count,
    )
    long_dine_in_percent = _share_percent(
        handover_stats.dine_in_long_orders_count,
        handover_stats.dine_in_orders_count,
    )

    lines = [
        f"\U0001F4CA <b>Ключевые метрики в заведении {unit_name}</b>",
        "",
        f"Выручка: {_format_revenue_change(current_revenue, previous_revenue)}",
        f"Производительность: {_format_metric_number(productivity_stats.sales_per_labor_hour)}",
        f"Продуктов на чел/час: {_format_metric_number(productivity_stats.products_per_labor_hour)}",
        "Сотрудники на смене:",
        f"        Кухня: {kitchen_on_shift}",
        f"        Курьеры: {couriers_on_shift}",
        f"Скорость доставки: {timedelta(seconds=delivery_stats.avg_delivery_seconds)}",
        f"Среднее время курьера с заказом в пути: {timedelta(seconds=delivery_stats.avg_trip_seconds)}",
        f"Время на полке: {timedelta(seconds=delivery_stats.avg_heated_shelf_seconds)}",
        f"Долгие доставки: {_format_percent(long_delivery_percent)}%",
        f"Заказов на курьера/час: {_format_metric_number(productivity_stats.orders_per_courier_labor_hour)}",
        f"Сертификаты: {_format_certificates(delivery_stats.late_orders_count)}",
        f"Время приготовления в ресторан: {avg_rest_prep}",
        f"Время приготовления на доставку: {avg_delivery_cooking}",
        f"Процент долгих приготовлений: {_format_percent(long_dine_in_percent)}%",
        f"Средняя оценка заказов, сделанных в ресторане: {_format_rating(customer_ratings.avg_dine_in_order_rate, customer_ratings.dine_in_rate_count)}",
        f"Средняя оценка заказов, сделанных на доставку: {_format_rating(customer_ratings.avg_delivery_order_rate, customer_ratings.delivery_rate_count)}",
        "",
        "#метрики",
    ]
    return "\n".join(lines)


def _format_revenue_change(current: Decimal, previous: Decimal) -> str:
    """Показывает выручку и дельту к такому же периоду прошлой недели."""
    current_value = int(current)
    previous_value = int(previous)
    try:
        percent = round((current_value * 100 / previous_value) - 100)
    except ZeroDivisionError:
        percent = 0

    if percent >= 0:
        return f"{current_value} <b>+{percent}%</b> \U0001F53C"
    return f"{current_value} <b>{percent}%</b> \U0001F53B"


def _format_metric_number(value: float) -> str:
    """Форматирует числовые метрики без лишних нулей, но с запятой как в старом отчете."""
    rounded = round(value, 2)
    if rounded.is_integer():
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".").replace(".", ",")


def _format_certificates(value: int) -> str:
    """Выделяет сертификаты, если их больше нуля."""
    if value <= 0:
        return "0"
    return f"{value} \U00002753"


def _average_timedelta(total_seconds: int, count: int) -> timedelta:
    if count <= 0:
        return timedelta(0)
    return timedelta(seconds=round(total_seconds / count))


def _share_percent(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return round(part * 100 / whole, 2)


def _format_percent(value: float) -> str:
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return text.replace(".", ",")


def _format_rating(value: float, count: int) -> str:
    """Форматирует среднюю оценку в виде `4,7 ⭐️ (141)`."""
    rating = f"{value:.1f}".replace(".", ",")
    return f"{rating} ⭐️ ({count})"


def _build_order_key(order_id: object, order_number: object) -> str:
    if isinstance(order_id, str) and order_id:
        return order_id
    if isinstance(order_number, str) and order_number:
        return order_number
    return ""


def _to_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_decimal(value: object) -> Decimal:
    try:
        if value is None:
            return Decimal("0")
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return Decimal("0")


def _calculate_long_deliveries_percent(
    *,
    statistics: Dict[str, object],
    delivery_times_by_order_id: Dict[str, int],
    handover: Dict[str, object],
    market_code: Optional[str],
) -> float:
    """Считает долю долгих доставок так же, как в отчете по курьерам."""
    stats_list = statistics.get("unitsStatistics", [])
    stats = stats_list[0] if stats_list else {}
    if not isinstance(stats, dict):
        stats = {}

    delivery_orders_count = to_int(stats.get("deliveryOrdersCount")) or 0
    if delivery_orders_count <= 0:
        return 0.0

    threshold_seconds = _long_delivery_threshold_seconds(market_code)
    long_deliveries_count = 0
    seen_order_ids: Set[str] = set()

    for order in handover.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Delivery":
            continue

        order_id = order.get("orderId")
        if not isinstance(order_id, str) or not order_id or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)

        delivery_time = delivery_times_by_order_id.get(order_id)
        if delivery_time is None:
            continue

        tracking_pending_time = to_int(order.get("trackingPendingTime")) or 0
        cooking_time = to_int(order.get("cookingTime")) or 0
        heated_shelf_time = to_int(order.get("heatedShelfTime")) or 0
        meeting_time = tracking_pending_time + cooking_time + heated_shelf_time

        if delivery_time + meeting_time > threshold_seconds:
            long_deliveries_count += 1

    return round(long_deliveries_count / delivery_orders_count * 100, 2)


def _long_delivery_threshold_seconds(market_code: Optional[str]) -> int:
    """Возвращает порог долгой доставки в секундах по рынку."""
    if (market_code or "").strip().lower() == "ru":
        return 45 * 60
    return 35 * 60


def _extract_market_code(properties: Optional[str]) -> Optional[str]:
    """Извлекает код рынка из `account.properties`."""
    normalized = (properties or "").strip().strip("/")
    if not normalized:
        return None
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return None
    return parts[-1].lower()


def _normalize_unit_uuids(value: object) -> List[str]:
    """Убирает пустые значения и дубли из списка заведений."""
    if not isinstance(value, list):
        return []

    result: List[str] = []
    seen: Set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result

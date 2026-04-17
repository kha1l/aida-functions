"""Ежедневный job по списаниям: причины, самое дорогое и самое частое списание."""

from __future__ import annotations

import asyncio
import html
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.models import Unit
from database.requests import select_report_subscriptions
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.unit_time import build_previous_local_day_window, resolve_unit_now

PAGE_SIZE = 500
PRICE_LOOKBACK_DAYS = 150
PRODUCTION_PRICE_LOOKBACK_DAYS = 30
MAX_CONCURRENT_UNITS = 15
TRIGGER_HOURS = set(schedule_local_hours("writeoff", (11,)))
SLICE_REASON_LABELS = {
    "ExpiredShowcaseTime": "Списания по времени с витрины",
    "ShowcaseWriteOff": "Списания кусочков",
}
MEASUREMENT_UNITS = {
    "Quantity": "шт.",
    "Kilogram": "кг",
    "Liter": "л",
    "Meter": "м",
}


@dataclass(frozen=True, slots=True)
class WriteOffEntry:
    """Одна запись списания продукта."""

    product_name: str
    reason: str
    amount: Decimal
    quantity: Decimal
    quantity_unit: str = "шт."


@dataclass(frozen=True, slots=True)
class WeeklyFrequentWriteOff:
    """Агрегат по самому частому списанию за неделю."""

    product_name: str
    amount: Decimal
    count: int
    quantity: Decimal
    quantity_unit: str


@dataclass(frozen=True, slots=True)
class IngredientCost:
    """Стоимость единицы списываемого ингредиента и его метаданные."""

    stock_item_id: str
    name: str
    unit_cost: Decimal
    quantity_unit: str


async def run_writeoff(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных writeoff-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "writeoff"),
    )
    if not snapshot.subscriptions:
        context.log.info("writeoff_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    unit_uuids = [
        unit_uuid
        for unit_uuid in snapshot.subs_by_unit.keys()
        if resolve_unit_now(snapshot.unit_for(unit_uuid)).hour in TRIGGER_HOURS
    ]
    if not unit_uuids:
        context.log.info(
            "writeoff_op: нет точек на локальные часы из SCHEDULE_WRITEOFF_HOURS",
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
                    context.log.warning("writeoff_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_writeoff_report(
                        dodo_api=dodo_api,
                        unit=unit,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="writeoff_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="writeoff_op",
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
                    post="writeoff",
                    text=report_text,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def _build_writeoff_report(
    *,
    dodo_api,
    unit: Optional[Unit],
    unit_uuid: str,
    unit_name: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Собирает один daily-отчет по списаниям для выбранной точки."""
    dt_start, dt_end = _build_previous_local_day_window(unit)
    # Частое списание считаем по скользящему окну последних 7 полных дней,
    # включая сам отчетный день.
    week_start = dt_start - timedelta(days=6)
    date_label = dt_start.strftime("%d.%m")

    day_params = _build_period_params(unit_uuid, dt_start, dt_end)
    week_params = _build_period_params(unit_uuid, week_start, dt_end)

    revenue_task = _fetch_revenue_total(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=day_params,
    )
    day_products_task = _fetch_product_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=day_params,
    )
    day_stock_items_task = _fetch_stock_item_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=day_params,
    )
    week_products_task = _fetch_product_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=week_params,
    )
    week_stock_items_task = _fetch_stock_item_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=week_params,
    )
    (
        revenue_result,
        day_products_result,
        day_stock_items_result,
        week_products_result,
        week_stock_items_result,
    ) = await asyncio.gather(
        revenue_task,
        day_products_task,
        day_stock_items_task,
        week_products_task,
        week_stock_items_task,
    )

    if revenue_result[0] is None:
        return None, revenue_result[1], revenue_result[2]
    if day_products_result[0] is None:
        return None, day_products_result[1], day_products_result[2]
    if day_stock_items_result[0] is None:
        return None, day_stock_items_result[1], day_stock_items_result[2]
    if week_products_result[0] is None:
        return None, week_products_result[1], week_products_result[2]
    if week_stock_items_result[0] is None:
        return None, week_stock_items_result[1], week_stock_items_result[2]

    stock_item_ids = _collect_stock_item_ids(
        day_product_rows=day_products_result[0],
        day_stock_item_rows=day_stock_items_result[0],
        week_product_rows=week_products_result[0],
        week_stock_item_rows=week_stock_items_result[0],
    )
    ingredient_costs_result = await _fetch_ingredient_costs(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        stock_item_ids=stock_item_ids,
        dt_end=dt_end,
    )
    if ingredient_costs_result[0] is None:
        return None, ingredient_costs_result[1], ingredient_costs_result[2]

    ingredient_costs = ingredient_costs_result[0]
    day_entries = _build_writeoff_entries(
        product_rows=day_products_result[0],
        stock_item_rows=day_stock_items_result[0],
        ingredient_costs=ingredient_costs,
    )
    week_entries = _build_writeoff_entries(
        product_rows=week_products_result[0],
        stock_item_rows=week_stock_items_result[0],
        ingredient_costs=ingredient_costs,
    )
    reason_totals = _build_reason_totals(
        product_rows=day_products_result[0],
        stock_item_rows=day_stock_items_result[0],
        ingredient_costs=ingredient_costs,
    )

    return _format_writeoff_message(
        unit_name=unit_name,
        date_label=date_label,
        revenue_total=revenue_result[0],
        day_writeoffs=day_entries,
        reason_totals=reason_totals,
        weekly_frequent=_find_weekly_frequent_writeoff(week_entries),
    ), None, 200


def _build_previous_local_day_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает границы предыдущего полного локального дня точки."""
    return build_previous_local_day_window(unit)


def _build_period_params(unit_uuid: str, dt_start: datetime, dt_end: datetime) -> Dict[str, str]:
    """Собирает query-параметры периода для Dodo API."""
    return {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }


async def _fetch_revenue_total(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[Decimal], Optional[str], Optional[int]]:
    """Считает выручку точки за период через accounting/sales."""
    skip = 0
    revenue_total = Decimal("0")

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
        rows = data.get("sales")
        if not isinstance(rows, list):
            rows = []

        for sale in rows:
            if not isinstance(sale, dict):
                continue
            for product in sale.get("products", []) or []:
                if not isinstance(product, dict):
                    continue
                revenue_total += _to_decimal(product.get("priceWithDiscount"))

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return revenue_total, None, 200


async def _fetch_product_writeoff_rows(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[List[dict]], Optional[str], Optional[int]]:
    """Загружает все списанные продукты за период."""
    return await _fetch_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        path="accounting/write-offs/products",
        params_base=params_base,
    )


async def _fetch_stock_item_writeoff_rows(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[List[dict]], Optional[str], Optional[int]]:
    """Загружает все списанные отдельно ингредиенты за период."""
    return await _fetch_writeoff_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        path="accounting/write-offs/stock-items",
        params_base=params_base,
    )


async def _fetch_writeoff_rows(
    *,
    dodo_api,
    account_id: int,
    path: str,
    params_base: Dict[str, str],
) -> tuple[Optional[List[dict]], Optional[str], Optional[int]]:
    """Универсально загружает строки списаний из write-offs endpoints."""
    skip = 0
    rows_result: List[dict] = []

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        response = await dodo_api.get(account_id, path, params=params)
        if not response.ok:
            return None, response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("writeOffs")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if isinstance(row, dict):
                rows_result.append(row)

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return rows_result, None, 200


async def _fetch_ingredient_costs(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    stock_item_ids: List[str],
    dt_end: datetime,
) -> tuple[Optional[Dict[str, IngredientCost]], Optional[str], Optional[int]]:
    """Собирает карту цен ингредиентов по той же цепочке источников, что и legacy-метрики."""
    if not stock_item_ids:
        return {}, None, 200

    result: Dict[str, IngredientCost] = {}
    stock_item_filter = set(stock_item_ids)

    error = await _apply_inventory_stock_costs(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        stock_item_filter=stock_item_filter,
        result=result,
    )
    if error is not None:
        return None, error[0], error[1]

    error = await _apply_production_costs(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        stock_item_filter=stock_item_filter,
        dt_start=dt_end - timedelta(days=PRODUCTION_PRICE_LOOKBACK_DAYS),
        dt_end=dt_end,
        result=result,
    )
    if error is not None:
        return None, error[0], error[1]

    error = await _fill_costs_from_incoming_stock_items(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        stock_item_filter=stock_item_filter,
        dt_start=dt_end - timedelta(days=PRICE_LOOKBACK_DAYS),
        dt_end=dt_end,
        result=result,
    )
    if error is not None:
        return None, error[0], error[1]

    error = await _fill_costs_from_stock_transfers(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        stock_item_filter=stock_item_filter,
        dt_start=dt_end - timedelta(days=PRICE_LOOKBACK_DAYS),
        dt_end=dt_end,
        result=result,
    )
    if error is not None:
        return None, error[0], error[1]

    return result, None, 200


async def _apply_inventory_stock_costs(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    stock_item_filter: set[str],
    result: Dict[str, IngredientCost],
) -> Optional[tuple[Optional[str], Optional[int]]]:
    """Первый источник цены: текущая стоимость остатков `balanceInMoney / quantity`."""
    skip = 0
    while True:
        response = await dodo_api.get(
            account_id,
            "accounting/inventory-stocks",
            params={
                "units": unit_uuid,
                "skip": skip,
                "take": PAGE_SIZE,
            },
        )
        if not response.ok:
            return response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("stocks")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            stock_item_id = _extract_stock_item_id(row)
            if not stock_item_id or stock_item_id not in stock_item_filter:
                continue
            quantity = _to_decimal(row.get("quantity"))
            balance_in_money = _to_decimal(row.get("balanceInMoney"))
            unit_cost = balance_in_money / quantity if quantity > 0 else Decimal("0")
            _set_ingredient_cost(
                result=result,
                stock_item_id=stock_item_id,
                unit_cost=unit_cost,
                name=str(row.get("name") or row.get("stockItemName") or stock_item_id).strip() or stock_item_id,
                quantity_unit=_measurement_unit_suffix(row.get("measurementUnit")),
                overwrite=True,
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return None


async def _apply_production_costs(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    stock_item_filter: set[str],
    dt_start: datetime,
    dt_end: datetime,
    result: Dict[str, IngredientCost],
) -> Optional[tuple[Optional[str], Optional[int]]]:
    """Второй источник цены: производство полуфабрикатов за последние 30 дней."""
    skip = 0
    while True:
        response = await dodo_api.get(
            account_id,
            "accounting/semi-finished-products-production",
            params={
                "units": unit_uuid,
                "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
                "skip": skip,
                "take": PAGE_SIZE,
            },
        )
        if not response.ok:
            return response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("productions")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            stock_item_id = _extract_stock_item_id(row)
            if not stock_item_id or stock_item_id not in stock_item_filter:
                continue
            _set_ingredient_cost(
                result=result,
                stock_item_id=stock_item_id,
                unit_cost=_to_decimal(row.get("price")),
                overwrite=True,
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return None


async def _fill_costs_from_incoming_stock_items(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    stock_item_filter: set[str],
    dt_start: datetime,
    dt_end: datetime,
    result: Dict[str, IngredientCost],
) -> Optional[tuple[Optional[str], Optional[int]]]:
    """Третий источник цены: приходы за последние 150 дней, только если цена еще нулевая."""
    skip = 0
    while True:
        response = await dodo_api.get(
            account_id,
            "accounting/incoming-stock-items",
            params={
                "units": unit_uuid,
                "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
                "skip": skip,
                "take": PAGE_SIZE,
            },
        )
        if not response.ok:
            return response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("incomingStockItems")
        if not isinstance(rows, list):
            rows = []

        # Идем как в legacy-коде: с конца страницы к началу.
        for row in reversed(rows):
            if not isinstance(row, dict):
                continue
            stock_item_id = _extract_stock_item_id(row)
            if not stock_item_id or stock_item_id not in stock_item_filter:
                continue
            price = _to_decimal(row.get("pricePerMeasurementUnitWithVat"))
            if price != 0 and _current_unit_cost(result, stock_item_id) == 0:
                _set_ingredient_cost(
                    result=result,
                    stock_item_id=stock_item_id,
                    unit_cost=price,
                    overwrite=True,
                )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return None


async def _fill_costs_from_stock_transfers(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    stock_item_filter: set[str],
    dt_start: datetime,
    dt_end: datetime,
    result: Dict[str, IngredientCost],
) -> Optional[tuple[Optional[str], Optional[int]]]:
    """Четвертый источник цены: полученные перемещения за последние 150 дней."""
    skip = 0
    while True:
        response = await dodo_api.get(
            account_id,
            "accounting/stock-transfers",
            params={
                "units": unit_uuid,
                "receivedFrom": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "receivedTo": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
                "statuses": "Received",
                "skip": skip,
                "take": PAGE_SIZE,
            },
        )
        if not response.ok:
            return response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("transfers")
        if not isinstance(rows, list):
            rows = []

        for row in reversed(rows):
            if not isinstance(row, dict):
                continue
            stock_item_id = _extract_stock_item_id(row)
            if not stock_item_id or stock_item_id not in stock_item_filter:
                continue
            price = _to_decimal(row.get("pricePerUnitWithVat"))
            if price != 0 and _current_unit_cost(result, stock_item_id) == 0:
                _set_ingredient_cost(
                    result=result,
                    stock_item_id=stock_item_id,
                    unit_cost=price,
                    overwrite=True,
                )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return None


def _set_ingredient_cost(
    *,
    result: Dict[str, IngredientCost],
    stock_item_id: str,
    unit_cost: Decimal,
    name: str | None = None,
    quantity_unit: str | None = None,
    overwrite: bool,
) -> None:
    """Обновляет цену ингредиента, сохраняя уже найденные имя и единицу измерения."""
    current = result.get(stock_item_id)
    if current is not None and not overwrite:
        return

    result[stock_item_id] = IngredientCost(
        stock_item_id=stock_item_id,
        name=(name or (current.name if current else "") or stock_item_id).strip() or stock_item_id,
        unit_cost=unit_cost,
        quantity_unit=(quantity_unit or (current.quantity_unit if current else "") or "").strip(),
    )


def _current_unit_cost(result: Dict[str, IngredientCost], stock_item_id: str) -> Decimal:
    """Возвращает текущую цену ингредиента или 0, если цена еще не найдена."""
    current = result.get(stock_item_id)
    return current.unit_cost if current is not None else Decimal("0")


def _collect_stock_item_ids(
    *,
    day_product_rows: List[dict],
    day_stock_item_rows: List[dict],
    week_product_rows: List[dict],
    week_stock_item_rows: List[dict],
) -> List[str]:
    """Собирает уникальный список ингредиентов из всех списаний за день и неделю."""
    ids: set[str] = set()
    for rows in (day_product_rows, week_product_rows):
        for row in rows:
            for stock_item in row.get("stockItems") or []:
                if not isinstance(stock_item, dict):
                    continue
                stock_item_id = _extract_stock_item_id(stock_item)
                if stock_item_id:
                    ids.add(stock_item_id)

    for rows in (day_stock_item_rows, week_stock_item_rows):
        for row in rows:
            stock_item_id = _extract_stock_item_id(row)
            if stock_item_id:
                ids.add(stock_item_id)

    return sorted(ids)


def _build_writeoff_entries(
    *,
    product_rows: List[dict],
    stock_item_rows: List[dict],
    ingredient_costs: Dict[str, IngredientCost],
) -> List[WriteOffEntry]:
    """Преобразует сырые списания продуктов и ингредиентов в единый список событий."""
    entries: List[WriteOffEntry] = []

    for row in product_rows:
        quantity = _to_decimal(row.get("quantity"))
        amount = _product_row_amount(row, ingredient_costs)
        entries.append(
            WriteOffEntry(
                product_name=str(row.get("productName") or "Неизвестный продукт").strip(),
                reason=str(row.get("reason") or "Unknown").strip() or "Unknown",
                amount=amount,
                quantity=quantity,
                quantity_unit="шт.",
            )
        )

    for row in stock_item_rows:
        stock_item_id = _extract_stock_item_id(row)
        cost_info = ingredient_costs.get(stock_item_id or "")
        quantity = _to_decimal(row.get("quantity"))
        product_name = str(
            row.get("stockItemName")
            or (cost_info.name if cost_info else "")
            or "Неизвестный ингредиент"
        ).strip()
        entries.append(
            WriteOffEntry(
                product_name=product_name,
                reason=str(row.get("reason") or "Unknown").strip() or "Unknown",
                amount=quantity * (cost_info.unit_cost if cost_info else Decimal("0")),
                quantity=quantity,
                quantity_unit=_measurement_unit_suffix(row.get("measurementUnit")) or (cost_info.quantity_unit if cost_info else ""),
            )
        )

    return entries


def _build_reason_totals(
    *,
    product_rows: List[dict],
    stock_item_rows: List[dict],
    ingredient_costs: Dict[str, IngredientCost],
) -> Dict[str, Decimal]:
    """Суммирует стоимость списаний по причинам через общие количества ингредиентов."""
    totals_by_reason_and_item: Dict[str, Dict[str, Decimal]] = defaultdict(dict)

    for row in product_rows:
        reason = str(row.get("reason") or "Unknown").strip() or "Unknown"
        for stock_item in row.get("stockItems") or []:
            if not isinstance(stock_item, dict):
                continue
            stock_item_id = _extract_stock_item_id(stock_item)
            if not stock_item_id:
                continue
            ingredient_quantity = _to_decimal(stock_item.get("quantity"))
            reason_bucket = totals_by_reason_and_item[reason]
            reason_bucket[stock_item_id] = reason_bucket.get(stock_item_id, Decimal("0")) + ingredient_quantity

    for row in stock_item_rows:
        reason = str(row.get("reason") or "Unknown").strip() or "Unknown"
        stock_item_id = _extract_stock_item_id(row)
        if not stock_item_id:
            continue
        ingredient_quantity = _to_decimal(row.get("quantity"))
        reason_bucket = totals_by_reason_and_item[reason]
        reason_bucket[stock_item_id] = reason_bucket.get(stock_item_id, Decimal("0")) + ingredient_quantity

    totals: Dict[str, Decimal] = {}
    for reason, items in totals_by_reason_and_item.items():
        amount = Decimal("0")
        for stock_item_id, quantity in items.items():
            cost_info = ingredient_costs.get(stock_item_id)
            if cost_info is None:
                continue
            amount += quantity * cost_info.unit_cost
        totals[reason] = amount
    return totals


def _product_row_amount(
    row: dict,
    ingredient_costs: Dict[str, IngredientCost],
) -> Decimal:
    """Считает стоимость списанного продукта через ингредиенты в его составе."""
    amount = Decimal("0")
    for stock_item in row.get("stockItems") or []:
        if not isinstance(stock_item, dict):
            continue
        stock_item_id = _extract_stock_item_id(stock_item)
        if not stock_item_id:
            continue
        cost_info = ingredient_costs.get(stock_item_id)
        if cost_info is None:
            continue
        amount += _to_decimal(stock_item.get("quantity")) * cost_info.unit_cost
    return amount


def _format_writeoff_message(
    *,
    unit_name: str,
    date_label: str,
    revenue_total: Decimal,
    day_writeoffs: List[WriteOffEntry],
    reason_totals: Dict[str, Decimal],
    weekly_frequent: Optional[WeeklyFrequentWriteOff],
) -> str:
    """Формирует итоговый daily-отчет по списаниям."""
    most_expensive = _find_most_expensive_writeoff(day_writeoffs)

    lines = [
        f"🗑 <b>Списания продуктов и сырья за {date_label} в заведении {html.escape(unit_name)}</b>",
        f"Выручка: {_format_money(revenue_total)}",
        "",
    ]

    if reason_totals:
        lines.append("<b>По причинам списания:</b>")
        lines.extend(_format_reason_lines(reason_totals, revenue_total))
    else:
        lines.append("<b>По причинам списания:</b>")
        lines.append("Списаний за день не найдено.")

    lines.append("")
    if most_expensive is not None:
        lines.append("<b>Самое дорогое списание:</b>")
        lines.append(f"{html.escape(most_expensive.product_name.strip())}:")
        lines.append(
            f"        Количество: {_format_quantity_line(most_expensive.quantity, most_expensive.quantity_unit, default_unit='шт.')}"
        )
        lines.append(f"        Стоимость: {_format_money(most_expensive.amount)}")
        lines.append(
            f"        % от выручки: {_format_percent(_share_percent(most_expensive.amount, revenue_total))}%"
        )
    else:
        lines.append("<b>Самое дорогое списание:</b>")
        lines.append("Не найдено")

    lines.append("")
    if weekly_frequent is not None:
        lines.append("<b>Самое частое списание за 7 дней:</b>")
        lines.append(f"{html.escape(weekly_frequent.product_name.strip())}:")
        lines.append(f"        Стоимость: {_format_money(weekly_frequent.amount)}")
        lines.append(
            f"        Количество: {_format_quantity_line(weekly_frequent.quantity, weekly_frequent.quantity_unit, default_unit='шт.')}"
        )
        lines.append(f"        Списаний: {weekly_frequent.count}")
    else:
        lines.append("<b>Самое частое списание за 7 дней:</b>")
        lines.append("Не найдено")

    lines.append("")
    lines.append("#списания")
    return "\n".join(lines)


def _format_reason_lines(
    reasons_totals: Dict[str, Decimal],
    revenue_total: Decimal,
) -> List[str]:
    """Форматирует блок причин списания с выровненными колонками."""
    rows = [
        (
            _label_reason(reason),
            _format_money(amount),
            f"({_format_percent(_share_percent(amount, revenue_total))}%)",
        )
        for reason, amount in sorted(reasons_totals.items(), key=lambda item: item[1], reverse=True)
    ]
    if not rows:
        return []

    name_width = max(len(name) for name, _, _ in rows)
    amount_width = max(len(amount) for _, amount, _ in rows)

    formatted: List[str] = []
    for name, amount, percent in rows:
        row = f"{name:<{name_width}}  {amount:>{amount_width}}  {percent}"
        formatted.append(f"<code>{html.escape(row).replace(' ', '&#160;')}</code>")
    return formatted


def _format_money(value: Decimal) -> str:
    """Форматирует денежные суммы с разделителями тысяч."""
    return f"{int(value):,}".replace(",", " ")

def _find_most_expensive_writeoff(rows: Iterable[WriteOffEntry]) -> Optional[WriteOffEntry]:
    """Находит самое дорогое разовое списание."""
    return max(rows, key=lambda item: item.amount, default=None)


def _find_weekly_frequent_writeoff(rows: Iterable[WriteOffEntry]) -> Optional[WeeklyFrequentWriteOff]:
    """Находит самое частое списание за 7 дней по названию продукта."""
    aggregated: Dict[str, WeeklyFrequentWriteOff] = {}
    for row in rows:
        current = aggregated.get(row.product_name)
        if current is None:
            aggregated[row.product_name] = WeeklyFrequentWriteOff(
                product_name=row.product_name,
                amount=row.amount,
                count=1,
                quantity=row.quantity,
                quantity_unit=row.quantity_unit,
            )
            continue
        aggregated[row.product_name] = WeeklyFrequentWriteOff(
            product_name=current.product_name,
            amount=current.amount + row.amount,
            count=current.count + 1,
            # Показываем не только число событий, но и общий объем списанного
            # товара за 7 дней.
            quantity=current.quantity + row.quantity,
            quantity_unit=current.quantity_unit,
        )

    if not aggregated:
        return None
    return max(
        aggregated.values(),
        key=lambda item: (item.count, item.amount, item.product_name),
    )


def _label_reason(reason: str) -> str:
    """Возвращает человекочитаемую подпись причины списания."""
    if reason in SLICE_REASON_LABELS:
        return SLICE_REASON_LABELS[reason]

    known = {
        "Marketing": "Маркетинг",
        "ShowcaseWriteOff": "Списания кусочков",
        "ExpiredShowcaseTime": "Списания по времени с витрины",
        "Expired": "Просрочка",
        "Defected": "Брак",
        "DamagedPackaging": "Брак упаковки",
        "HumanElement": "Человеческий фактор",
    }
    return known.get(reason, reason)


def _share_percent(part: Decimal, total: Decimal) -> float:
    """Считает долю части от общего значения в процентах."""
    if total <= 0:
        return 0.0
    return float((part / total) * Decimal("100"))


def _format_percent(value: float) -> str:
    """Форматирует процент с запятой и без лишних нулей."""
    rounded = round(value, 2)
    if float(rounded).is_integer():
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".").replace(".", ",")


def _format_quantity(quantity: Decimal) -> str:
    """Форматирует количество без лишних нулей."""
    if quantity == quantity.to_integral_value():
        return str(int(quantity))
    return f"{float(quantity):g}"


def _format_quantity_line(
    quantity: Decimal,
    quantity_unit: str | None = None,
    *,
    default_unit: str = "",
) -> str:
    """Собирает строку количества с опциональной единицей измерения."""
    suffix = (quantity_unit or default_unit or "").strip()
    if suffix:
        return f"{_format_quantity(quantity)} {suffix}"
    return _format_quantity(quantity)


def _extract_stock_item_id(row: dict) -> str:
    """Достает идентификатор ингредиента из разных форм API."""
    return str(row.get("stockItemId") or row.get("id") or "").strip()


def _measurement_unit_suffix(value: object) -> str:
    """Преобразует код единицы измерения в человекочитаемый суффикс."""
    return MEASUREMENT_UNITS.get(str(value or "").strip(), "")


def _to_decimal(value: object) -> Decimal:
    """Безопасно приводит число к Decimal."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")

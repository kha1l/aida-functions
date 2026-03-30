"""Регулярный job для выгрузки данных store widget без legacy public API."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence

import aiohttp
from dagster import OpExecutionContext

from config import dodo_api_settings, store_widget_settings
from database.requests import (
    load_accounts_and_tokens_bulk,
    load_franchisee_group_uuids_by_unit,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_active_unit_uuids,
)
from functions.shared.report_job_runtime import log_auth_error, log_final_unit_error
from functions.shared.unit_time import resolve_unit_now


COMPARE_PERIOD_DAYS = 7
PAGE_SIZE = 500
MAX_CONCURRENT_UNITS = 15
CURRENCY_SYMBOLS = {
    "ae": ".د.إ",
    "ru": "₽",
    "kz": "₸",
    "lt": "LTL",
    "pl": "zł",
}


@dataclass(frozen=True, slots=True)
class WidgetUnitStats:
    """Выручка одной точки для выгрузки в widget."""

    unit_uuid: str
    unit_name: str
    country: str
    symbol: str
    current_revenue: Decimal
    previous_revenue: Decimal

    @property
    def percent_change(self) -> int:
        if self.previous_revenue == 0:
            return 100 if self.current_revenue > 0 else 0
        return int((self.current_revenue * Decimal("100") / self.previous_revenue) - Decimal("100"))


@dataclass(frozen=True, slots=True)
class StoreWidgetUploadItem:
    """Один элемент dataToUpload для store-api/storage."""

    unit_uuid: str
    current_revenue: str
    previous_revenue: str
    percent_change: str
    units_revenue: list[dict[str, str]]


async def run_widget(context: OpExecutionContext) -> None:
    """Собирает актуальные данные для виджета и выгружает их в Store API."""
    db = context.resources.db
    dodo_api = context.resources.dodo_api
    widget_cfg = store_widget_settings()

    if not widget_cfg.client_id or not widget_cfg.client_secret or not widget_cfg.model_id:
        context.log.error(
            "widget_op: не заданы обязательные настройки Store API (client_id/client_secret/model_id)"
        )
        return

    unit_uuids = await select_active_unit_uuids(db)
    if not unit_uuids:
        context.log.info("widget_op: не найдено точек с активными аккаунтами/токенами")
        return

    units, candidate_accounts_by_unit, franchisee_group_by_unit = await asyncio.gather(
        load_units_by_uuids(db, unit_uuids),
        select_account_ids_for_units(db, unit_uuids),
        load_franchisee_group_uuids_by_unit(db, unit_uuids),
    )
    all_account_ids = {
        account_id
        for account_ids in candidate_accounts_by_unit.values()
        for account_id in account_ids
    }
    account_auth_map = await load_accounts_and_tokens_bulk(db, all_account_ids)
    dodo_api.prime_account_token_cache(account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    unit_stats_by_uuid: Dict[str, WidgetUnitStats] = {}
    unit_stats_lock = asyncio.Lock()

    async with AsyncExitStack() as stack:
        timeout = aiohttp.ClientTimeout(total=dodo_api_settings().timeout_seconds)
        dodo_session = await stack.enter_async_context(aiohttp.ClientSession(timeout=timeout, trust_env=True))
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests)),
        )
        store_session = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=widget_cfg.timeout_seconds), trust_env=True)
        )

        try:
            async def process_unit(unit_uuid: str) -> None:
                async with semaphore:
                    unit = units.get(unit_uuid)
                    if unit is None:
                        return

                    local_now = resolve_unit_now(unit)
                    candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, [])
                    if not candidate_accounts:
                        context.log.warning("widget_op: нет токенов для unit %s", unit_uuid)
                        return

                    stats: Optional[WidgetUnitStats] = None
                    last_error_html: Optional[str] = None
                    last_error_status: Optional[int] = None

                    for account_id in candidate_accounts:
                        stats, last_error_html, last_error_status = await _build_widget_stats(
                            dodo_api=dodo_api,
                            unit_uuid=unit_uuid,
                            unit_name=unit.name if unit.name else unit_uuid,
                            country=(unit.country or "").lower(),
                            account_id=account_id,
                            local_now=local_now,
                        )
                        if last_error_html is None and last_error_status == 200:
                            break
                        log_auth_error(
                            context,
                            log_prefix="widget_op",
                            unit_uuid=unit_uuid,
                            account_id=account_id,
                            error_html=last_error_html,
                            error_status=last_error_status,
                        )

                    if stats is None:
                        log_final_unit_error(
                            context,
                            log_prefix="widget_op",
                            unit_uuid=unit_uuid,
                            error_html=last_error_html,
                            error_status=last_error_status,
                            empty_message="нет данных",
                        )
                        return

                    async with unit_stats_lock:
                        unit_stats_by_uuid[unit_uuid] = stats

            await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))

            if not unit_stats_by_uuid:
                context.log.warning("widget_op: не удалось собрать данные ни по одной точке")
                return

            uploads = _build_upload_items(
                unit_stats_by_uuid=unit_stats_by_uuid,
                franchisee_group_by_unit=franchisee_group_by_unit,
            )
            if not uploads:
                context.log.warning("widget_op: не сформировано ни одного payload для store-api")
                return

            token = await _fetch_store_widget_token(store_session, widget_cfg)
            for chunk in _chunked(uploads, widget_cfg.upload_chunk_size):
                await _upload_store_widget_chunk(store_session, widget_cfg, token, chunk)

            context.log.info(
                "widget_op: успешно выгружено элементов в store-api/storage: %s",
                len(uploads),
            )
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()


async def _build_widget_stats(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    country: str,
    account_id: int,
    local_now: datetime,
) -> tuple[Optional[WidgetUnitStats], Optional[str], Optional[int]]:
    """Считает выручку точки на текущий момент и за такой же срез прошлой недели."""
    current_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    current_end = local_now.replace(microsecond=0)
    previous_start = current_start - timedelta(days=COMPARE_PERIOD_DAYS)
    previous_end = current_end - timedelta(days=COMPARE_PERIOD_DAYS)

    current_result, previous_result = await asyncio.gather(
        _fetch_revenue_total(
            dodo_api=dodo_api,
            account_id=account_id,
            unit_uuid=unit_uuid,
            dt_start=current_start,
            dt_end=current_end,
        ),
        _fetch_revenue_total(
            dodo_api=dodo_api,
            account_id=account_id,
            unit_uuid=unit_uuid,
            dt_start=previous_start,
            dt_end=previous_end,
        ),
    )

    if current_result[0] is None:
        return None, current_result[1], current_result[2]
    if previous_result[0] is None:
        return None, previous_result[1], previous_result[2]

    return (
        WidgetUnitStats(
            unit_uuid=unit_uuid,
            unit_name=unit_name,
            country=country,
            symbol=CURRENCY_SYMBOLS.get(country, ""),
            current_revenue=current_result[0],
            previous_revenue=previous_result[0],
        ),
        None,
        200,
    )


async def _fetch_revenue_total(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[Optional[Decimal], Optional[str], Optional[int]]:
    """Суммирует выручку по accounting/sales за указанный период."""
    skip = 0
    revenue_total = Decimal("0")

    while True:
        result = await dodo_api.get(
            account_id,
            "accounting/sales",
            params={
                "units": unit_uuid,
                "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
                "take": PAGE_SIZE,
                "skip": skip,
            },
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data if isinstance(result.data, dict) else {}
        rows = data.get("sales")
        if not isinstance(rows, list):
            rows = []

        for sale in rows:
            if not isinstance(sale, dict):
                continue
            for product in sale.get("products") or []:
                if isinstance(product, dict):
                    revenue_total += _to_decimal(product.get("priceWithDiscount"))

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return revenue_total, None, 200


def _build_upload_items(
    *,
    unit_stats_by_uuid: Dict[str, WidgetUnitStats],
    franchisee_group_by_unit: Dict[str, str],
) -> list[StoreWidgetUploadItem]:
    """Формирует элементы выгрузки в store-api/storage по каждой точке."""
    groups: dict[str, list[WidgetUnitStats]] = defaultdict(list)
    for unit_uuid, stats in unit_stats_by_uuid.items():
        group_key = franchisee_group_by_unit.get(unit_uuid) or unit_uuid
        groups[group_key].append(stats)

    uploads: list[StoreWidgetUploadItem] = []
    for unit_uuid, stats in sorted(unit_stats_by_uuid.items(), key=lambda item: item[1].unit_name.lower()):
        group_key = franchisee_group_by_unit.get(unit_uuid) or unit_uuid
        group_stats = sorted(groups[group_key], key=lambda item: item.unit_name.lower())

        total_current = sum((item.current_revenue for item in group_stats), Decimal("0"))
        total_previous = sum((item.previous_revenue for item in group_stats), Decimal("0"))
        total_percent = _calc_percent_change(total_current, total_previous)
        symbol = stats.symbol or ""

        uploads.append(
            StoreWidgetUploadItem(
                unit_uuid=unit_uuid,
                current_revenue=f"{_format_money(total_current)} {symbol}".strip(),
                previous_revenue=f"{_format_money(total_previous)} {symbol}".strip(),
                percent_change=_format_percent(total_percent),
                units_revenue=[
                    {
                        "unitName": item.unit_name,
                        "unitRevenue": f"{_format_money(item.current_revenue)} {item.symbol or symbol}".strip(),
                        "unitRevenuePercentageDifference": _format_percent(item.percent_change),
                    }
                    for item in group_stats
                ],
            )
        )
    return uploads


async def _fetch_store_widget_token(session: aiohttp.ClientSession, cfg) -> str:
    """Получает OAuth-токен для доступа к store-api."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "user-agent": "aida_functions/widget",
    }
    data = {
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "scope": cfg.scope,
        "grant_type": "client_credentials",
    }

    last_error: Optional[str] = None
    for attempt in range(1, cfg.retries + 1):
        async with session.post(cfg.token_url, data=data, headers=headers) as response:
            response_text = await response.text()
            if response.status in {429, 500} and attempt < cfg.retries:
                await asyncio.sleep(min(attempt * 2, 10))
                continue
            try:
                payload = json.loads(response_text or "{}")
            except json.JSONDecodeError as exc:
                last_error = f"invalid token json: {exc}"
                if attempt < cfg.retries:
                    await asyncio.sleep(min(attempt * 2, 10))
                    continue
                break

            token = payload.get("access_token")
            if response.status == 200 and token:
                return str(token)
            last_error = f"status={response.status} body={response_text[:500]}"
            if attempt < cfg.retries:
                await asyncio.sleep(min(attempt * 2, 10))
                continue

    raise RuntimeError(f"widget_op: не удалось получить store-api токен: {last_error}")


async def _upload_store_widget_chunk(
    session: aiohttp.ClientSession,
    cfg,
    token: str,
    chunk: Sequence[StoreWidgetUploadItem],
) -> None:
    """Отправляет один chunk элементов в store-api/storage."""
    payload = {
        "dataToUpload": [
            {
                "modelId": cfg.model_id,
                "unitId": item.unit_uuid,
                "data": {
                    "currentWeekRestaurantChainRevenue": item.current_revenue,
                    "previousWeekRestaurantChainRevenue": item.previous_revenue,
                    "restaurantChainRevenuePercentageDifference": item.percent_change,
                    "unitsRevenue": item.units_revenue,
                },
            }
            for item in chunk
        ]
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    last_error: Optional[str] = None
    for attempt in range(1, cfg.retries + 1):
        async with session.post(cfg.storage_url, headers=headers, json=payload) as response:
            response_text = await response.text()
            if response.status in {429, 500} and attempt < cfg.retries:
                await asyncio.sleep(min(attempt * 2, 10))
                continue
            if 200 <= response.status < 300:
                return
            last_error = f"status={response.status} body={response_text[:1000]}"
            if attempt < cfg.retries:
                await asyncio.sleep(min(attempt * 2, 10))
                continue

    raise RuntimeError(f"widget_op: store-api/storage upload failed: {last_error}")


def _chunked(items: Sequence[StoreWidgetUploadItem], size: int) -> Iterable[Sequence[StoreWidgetUploadItem]]:
    """Режет список элементов на chunks фиксированного размера."""
    step = max(1, int(size))
    for index in range(0, len(items), step):
        yield items[index:index + step]


def _to_decimal(value: Any) -> Decimal:
    """Нормализует числовое значение в Decimal."""
    try:
        if value is None or value == "":
            return Decimal("0")
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _calc_percent_change(current: Decimal, previous: Decimal) -> int:
    """Считает процентное изменение текущей выручки относительно прошлой недели."""
    if previous == 0:
        return 100 if current > 0 else 0
    return int((current * Decimal("100") / previous) - Decimal("100"))


def _format_percent(value: int) -> str:
    """Форматирует процент с ведущим плюсом для положительных значений."""
    return f"+{value}%" if value > 0 else f"{value}%"


def _format_money(value: Decimal) -> str:
    """Форматирует денежную сумму с разделителями тысяч."""
    return f"{int(value):,}".replace(",", " ")

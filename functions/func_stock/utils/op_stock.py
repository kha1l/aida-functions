"""Регулярный job по критичным складским остаткам с локальными часами из env."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.requests import (
    ReportSubscription,
    load_accounts_and_tokens_bulk,
    load_chat_languages,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_report_subscriptions,
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
from functions.shared.unit_time import resolve_unit_now
from messaging import build_payload

TRIGGER_HOURS = set(schedule_local_hours("stock", (8, 15)))
MAX_CONCURRENT_REPORTS = 15
PAGE_SIZE = 500

MEASUREMENT_UNITS = {
    "Quantity": "шт",
    "Kilogram": "кг",
    "Liter": "л",
    "Meter": "м",
}


@dataclass(frozen=True, slots=True)
class StockReportTarget:
    """Группа подписчиков, которым нужен один и тот же stock-отчет."""

    unit_uuid: str
    unit_name: str
    params: Optional[str]
    subscribers: List[ReportSubscription]
    candidate_accounts: List[int]
    local_now: datetime


async def run_stock(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку отчетов по критичным складским остаткам."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "stock"),
    )
    if not snapshot.subscriptions:
        context.log.info("stock_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    # Для stock params влияют на текст отчета, поэтому группируем по (unit, params).
    subscriptions_by_key: Dict[Tuple[str, str], List[ReportSubscription]] = {}
    for sub in snapshot.subscriptions:
        params_key = _normalize_params_key(sub.params)
        subscriptions_by_key.setdefault((sub.unit_uuid, params_key), []).append(sub)

    targets: List[StockReportTarget] = []
    for (unit_uuid, _params_key), subscribers in subscriptions_by_key.items():
        unit = snapshot.unit_for(unit_uuid)
        local_now = resolve_unit_now(unit)
        if local_now.hour not in TRIGGER_HOURS:
            continue

        candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
        if not candidate_accounts:
            context.log.warning("stock_op: нет токенов для unit %s", unit_uuid)
            continue

        targets.append(
            StockReportTarget(
                unit_uuid=unit_uuid,
                unit_name=snapshot.unit_name_for(unit_uuid),
                params=subscribers[0].params,
                subscribers=subscribers,
                candidate_accounts=candidate_accounts,
                local_now=local_now,
            )
        )

    if not targets:
        context.log.info("stock_op: нет точек на локальные часы из SCHEDULE_STOCK_HOURS")
        dodo_api.clear_account_token_cache()
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REPORTS)
    publish_lock = asyncio.Lock()

    async with report_job_runtime(dodo_api, rabbitmq):
        async def process_target(target: StockReportTarget) -> None:
            async with semaphore:
                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in target.candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_stock_report(
                        dodo_api=dodo_api,
                        unit_uuid=target.unit_uuid,
                        unit_name=target.unit_name,
                        account_id=account_id,
                        categories=_parse_categories(target.params),
                        local_now=target.local_now,
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="stock_op",
                        unit_uuid=target.unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="stock_op",
                        unit_uuid=target.unit_uuid,
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
                    subscribers=target.subscribers,
                    post="stock",
                    text=report_text,
                    unit_uuid=target.unit_uuid,
                )

        await asyncio.gather(*(process_target(target) for target in targets))


async def run_stock_manual(context: OpExecutionContext) -> None:
    """Собирает оперативный отчет по складу для выбранных пользователем заведений."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    unit_uuids = _normalize_unit_uuids(context.op_config.get("unit_uuids"))
    if not unit_uuids:
        context.log.warning("stock_manual_op: не переданы unit_uuids")
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
    chat_languages = await load_chat_languages(db, [chat_id])
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
            local_now = resolve_unit_now(unit)
            candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, [])
            if not candidate_accounts:
                context.log.warning("stock_manual_op: нет токенов для unit %s", unit_uuid)
                continue

            report_text: Optional[str] = None
            last_error_html: Optional[str] = None
            last_error_status: Optional[int] = None

            for account_id in candidate_accounts:
                report_text, last_error_html, last_error_status = await _build_stock_report(
                    dodo_api=dodo_api,
                    unit_uuid=unit_uuid,
                    unit_name=unit_name,
                    account_id=account_id,
                    categories=[],
                    local_now=local_now,
                )
                if last_error_html is None and last_error_status == 200:
                    break
                log_auth_error(
                    context,
                    log_prefix="stock_manual_op",
                    unit_uuid=unit_uuid,
                    account_id=account_id,
                    error_html=last_error_html,
                    error_status=last_error_status,
                )

            if not report_text:
                log_final_unit_error(
                    context,
                    log_prefix="stock_manual_op",
                    unit_uuid=unit_uuid,
                    error_html=last_error_html,
                    error_status=last_error_status,
                    empty_message="нет данных",
                )
                continue

            payload = build_payload(
                chat_id=chat_id,
                topic_id=topic_id,
                post="stock",
                text=translate_report_text("stock", report_language, report_text),
                unit_uuid=unit_uuid,
            )
            await rabbitmq.publish_async(payload)


async def _build_stock_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    categories: List[str],
    local_now: datetime,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Строит текст отчета по остаткам для одной точки и одного набора категорий."""
    stocks, error_html, error_status = await _fetch_inventory_stocks(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        categories=categories,
    )
    if stocks is None:
        return None, error_html, error_status

    return _format_stock_message(
        unit_name=unit_name,
        local_now=local_now,
        stocks=stocks,
    ), None, 200


async def _fetch_inventory_stocks(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    categories: List[str],
) -> tuple[Optional[List[dict]], Optional[str], Optional[int]]:
    """Выгружает все складские остатки по точке с учетом выбранных категорий."""
    skip = 0
    stocks: List[dict] = []

    while True:
        params = {
            "units": unit_uuid,
            "skip": skip,
            "take": PAGE_SIZE,
        }
        if categories:
            params["categories"] = ",".join(categories)

        result = await dodo_api.get(
            account_id,
            "accounting/inventory-stocks",
            params=params,
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data if isinstance(result.data, dict) else {}
        for item in data.get("stocks", []) or []:
            if isinstance(item, dict):
                stocks.append(item)

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return stocks, None, 200


def _format_stock_message(
    *,
    unit_name: str,
    local_now: datetime,
    stocks: List[dict],
) -> str:
    """Формирует текст отчета по проблемным складским остаткам."""
    lines = [
        f"\U0001F9C0 Складские остатки на {local_now.strftime('%d.%m %H:%M:%S')}",
        "",
        f"<b>{unit_name}</b>",
    ]

    issue_lines: List[str] = []
    for item in stocks:
        message = _build_stock_issue_line(item)
        if message:
            issue_lines.append(message)

    if issue_lines:
        lines.extend(issue_lines)
    else:
        lines.append("    \u2705 Критичных позиций не найдено")

    lines.append("")
    lines.append("#складскиеОстатки")

    return "\n".join(lines)


def _build_stock_issue_line(item: dict) -> str:
    """Возвращает текст только для критичных или подозрительных складских позиций."""
    days_until_runs_out = to_int(item.get("daysUntilBalanceRunsOut"))
    avg_weekday_expense = _to_float(item.get("avgWeekdayExpense"))
    avg_weekend_expense = _to_float(item.get("avgWeekendExpense"))
    quantity = _to_float(item.get("quantity"))

    if days_until_runs_out is None or days_until_runs_out > 2:
        return ""
    if avg_weekday_expense == 0:
        return ""

    name = str(item.get("name") or "Неизвестная позиция")
    unit_suffix = MEASUREMENT_UNITS.get(str(item.get("measurementUnit") or ""), "")
    quantity_text = _format_quantity(quantity)

    if days_until_runs_out == 0:
        if quantity < 0:
            return f"    ⚠️ {name} осталось {quantity_text}{unit_suffix} - некорректный остаток"
        if avg_weekday_expense < 0 or avg_weekend_expense < 0:
            return f"    ❔ {name} осталось {quantity_text}{unit_suffix} - расход по dodois меньше нуля"
        if quantity <= avg_weekday_expense:
            return f"    ‼️ {name} осталось {quantity_text}{unit_suffix} - хватит менее чем на день"
        return ""

    if days_until_runs_out == 1:
        return f"    ‼️ {name} осталось {quantity_text}{unit_suffix} - хватит менее чем на день"

    return f"    ❕ {name} осталось {quantity_text}{unit_suffix} - хватит на {days_until_runs_out} дня"


def _normalize_params_key(value: Optional[str]) -> str:
    """Приводит сырые params из БД к стабильному ключу группировки."""
    categories = _parse_categories(value)
    if not categories:
        return ""
    return ",".join(categories)


def _parse_categories(value: Optional[str]) -> List[str]:
    """Разбирает категории stock из `orders.params`, включая старый формат `prefix|a,b`."""
    if not value:
        return []

    raw = value
    if "|" in raw:
        _prefix, raw = raw.split("|", 1)

    categories: List[str] = []
    seen = set()
    for part in raw.split(","):
        cleaned = part.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        categories.append(cleaned)
    return categories


def _format_quantity(value: float) -> str:
    """Форматирует количество без лишних нулей."""
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    if text == "-0":
        return "0"
    return text


def _to_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_unit_uuids(value: object) -> List[str]:
    """Убирает пустые значения и дубли из списка заведений."""
    if not isinstance(value, list):
        return []

    result: List[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result

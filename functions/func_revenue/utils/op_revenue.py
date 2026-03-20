"""Регулярный job по выручке сети с группировкой по франчайзи."""

from __future__ import annotations

import asyncio
import html
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.requests import (
    load_accounts_and_tokens_bulk,
    load_chat_languages,
    load_units_by_uuids,
    load_franchisee_group_uuids_by_unit,
    select_account_ids_for_units,
    select_report_subscriptions,
)
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    report_job_runtime,
)
from functions.shared.report_text_i18n import translate_report_text
from functions.shared.unit_time import resolve_unit_now
from messaging import build_payload

TRIGGER_HOURS = set(schedule_local_hours("revenue", (16, 23)))
MAX_CONCURRENT_UNITS = 15
PAGE_SIZE = 500
COMPARE_PERIOD_DAYS = 7


@dataclass(frozen=True, slots=True)
class RevenueUnitStats:
    """Выручка одной точки за текущий период и за аналогичный период прошлой недели."""

    unit_uuid: str
    unit_name: str
    current_revenue: Decimal
    previous_revenue: Decimal


async def run_revenue(context: OpExecutionContext) -> None:
    """Собирает выручку по точкам и отправляет агрегированный отчет по сети."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "revenue"),
    )
    if not snapshot.subscriptions:
        context.log.info("revenue_op: нет подписок")
        return

    franchisee_group_by_unit = await load_franchisee_group_uuids_by_unit(
        db,
        snapshot.subs_by_unit.keys(),
    )
    if not franchisee_group_by_unit:
        context.log.warning(
            "revenue_op: special.franchisee не вернула group_uuid, используем fallback по unit_uuid"
        )
    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    unit_stats_by_uuid: Dict[str, RevenueUnitStats] = {}
    unit_stats_lock = asyncio.Lock()
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

                candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
                if not candidate_accounts:
                    context.log.warning("revenue_op: нет токенов для unit %s", unit_uuid)
                    return

                stats: Optional[RevenueUnitStats] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    stats, last_error_html, last_error_status = await _build_revenue_stats(
                        dodo_api=dodo_api,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                        local_now=local_now,
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="revenue_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="revenue_op",
                        unit_uuid=unit_uuid,
                        error_html=last_error_html,
                        error_status=last_error_status,
                        empty_message="нет данных",
                    )
                    return

                if stats is None:
                    return

                async with unit_stats_lock:
                    unit_stats_by_uuid[unit_uuid] = stats

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))

        grouped_units_by_target: Dict[Tuple[int, Optional[int], str, str], Dict[str, RevenueUnitStats]] = defaultdict(dict)
        for unit_uuid, stats in unit_stats_by_uuid.items():
            network_key = franchisee_group_by_unit.get(unit_uuid) or unit_uuid
            for sub in snapshot.subscribers_for(unit_uuid):
                target_key = (sub.chat_id, sub.topic_id, network_key, sub.language)
                grouped_units_by_target[target_key][unit_uuid] = stats

        for (chat_id, topic_id, _network_key, language), grouped_units in grouped_units_by_target.items():
            if not grouped_units:
                continue
            message = translate_report_text(
                "revenue",
                language,
                _format_revenue_network_message(grouped_units.values()),
            )
            payload = build_payload(
                chat_id=chat_id,
                topic_id=topic_id,
                post="revenue",
                text=message,
                unit_uuid=None,
            )
            async with publish_lock:
                await rabbitmq.publish_async(payload)


async def run_revenue_manual(context: OpExecutionContext) -> None:
    """Собирает оперативный отчет по выручке для выбранных пользователем заведений."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    unit_uuids = _normalize_unit_uuids(context.op_config.get("unit_uuids"))
    if not unit_uuids:
        context.log.warning("revenue_manual_op: не переданы unit_uuids")
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
    chat_languages = await load_chat_languages(db, [chat_id])
    report_language = chat_languages.get(chat_id, "ru")

    async with report_job_runtime(dodo_api, rabbitmq):
        unit_stats: List[RevenueUnitStats] = []

        for unit_uuid in unit_uuids:
            unit = units.get(unit_uuid)
            local_now = resolve_unit_now(unit)
            candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, [])
            if not candidate_accounts:
                context.log.warning("revenue_manual_op: нет токенов для unit %s", unit_uuid)
                continue

            stats: Optional[RevenueUnitStats] = None
            last_error_html: Optional[str] = None
            last_error_status: Optional[int] = None

            for account_id in candidate_accounts:
                stats, last_error_html, last_error_status = await _build_revenue_stats(
                    dodo_api=dodo_api,
                    unit_uuid=unit_uuid,
                    unit_name=units.get(unit_uuid).name if units.get(unit_uuid) and units.get(unit_uuid).name else unit_uuid,
                    account_id=account_id,
                    local_now=local_now,
                )
                if last_error_html is None and last_error_status == 200:
                    break
                log_auth_error(
                    context,
                    log_prefix="revenue_manual_op",
                    unit_uuid=unit_uuid,
                    account_id=account_id,
                    error_html=last_error_html,
                    error_status=last_error_status,
                )

            if stats is None:
                log_final_unit_error(
                    context,
                    log_prefix="revenue_manual_op",
                    unit_uuid=unit_uuid,
                    error_html=last_error_html,
                    error_status=last_error_status,
                    empty_message="нет данных",
                )
                continue
            unit_stats.append(stats)

        if not unit_stats:
            context.log.warning("revenue_manual_op: не удалось собрать данные ни по одной точке")
            return

        payload = build_payload(
            chat_id=chat_id,
            topic_id=context.op_config.get("topic_id"),
            post="revenue",
            text=translate_report_text(
                "revenue",
                report_language,
                _format_revenue_network_message(unit_stats),
            ),
            unit_uuid=None,
        )
        await rabbitmq.publish_async(payload)


async def _build_revenue_stats(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    local_now: datetime,
) -> tuple[Optional[RevenueUnitStats], Optional[str], Optional[int]]:
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
        RevenueUnitStats(
            unit_uuid=unit_uuid,
            unit_name=unit_name,
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
    """Суммирует выручку по `accounting/sales` за указанный период."""
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


def _format_revenue_network_message(stats_items: List[RevenueUnitStats] | Tuple[RevenueUnitStats, ...] | object) -> str:
    """Формирует итоговый Telegram-текст по сети с выровненными колонками."""
    stats_list = sorted(list(stats_items), key=lambda item: item.unit_name.lower())
    total_current = sum((item.current_revenue for item in stats_list), Decimal("0"))
    total_previous = sum((item.previous_revenue for item in stats_list), Decimal("0"))

    display_rows = [
        (
            item.unit_name,
            _format_money(item.current_revenue),
            _format_change(item.current_revenue, item.previous_revenue),
        )
        for item in stats_list
    ]
    total_row = (
        "Итого:",
        _format_money(total_current),
        _format_change(total_current, total_previous),
    )

    name_width = max([len(row[0]) for row in display_rows] + [len(total_row[0])])
    revenue_width = max([len(row[1]) for row in display_rows] + [len(total_row[1])])

    lines = ["📊 <b>Выручка по сети:</b>", "<pre>"]
    for name, revenue, change in display_rows:
        lines.append(
            f"{html.escape(name):<{name_width}} | {revenue:>{revenue_width}} | {change}"
        )
    lines.append("")
    lines.append(
        f"{html.escape(total_row[0]):<{name_width}} | {total_row[1]:>{revenue_width}} | {total_row[2]}"
    )
    lines.append("</pre>")
    lines.append("#выручкаПоСети")
    return "\n".join(lines)


def _format_money(value: Decimal) -> str:
    """Форматирует сумму с пробелами между тысячами."""
    return f"{int(value):,}".replace(",", " ")


def _format_change(current: Decimal, previous: Decimal) -> str:
    """Форматирует процент изменения к аналогичному периоду прошлой недели."""
    if previous <= 0:
        percent = 0
    else:
        percent = round((current * Decimal("100") / previous) - Decimal("100"))

    if percent > 0:
        return f"+{int(percent)}%"
    if percent < 0:
        return f"{int(percent)}% 🔻"
    return "0%"


def _to_decimal(value: object) -> Decimal:
    """Безопасно приводит значение из Dodo API к Decimal."""
    try:
        return Decimal(str(value or 0))
    except Exception:  # noqa: BLE001
        return Decimal("0")


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

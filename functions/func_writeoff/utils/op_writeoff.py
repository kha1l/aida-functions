"""Ежедневный job по списаниям: причины, самое дорогое и самое частое списание."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional

from dagster import OpExecutionContext

from database.models import Unit
from database.requests import select_report_subscriptions
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.unit_time import build_previous_local_day_window

PAGE_SIZE = 500
MAX_CONCURRENT_UNITS = 15
SLICE_REASON_LABELS = {
    "ExpiredShowcaseTime": "Списания по времени с витрины",
    "ShowcaseWriteOff": "Списания кусочков",
}


@dataclass(frozen=True, slots=True)
class WriteOffEntry:
    """Одна запись списания продукта."""

    product_name: str
    reason: str
    amount: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class WeeklyFrequentWriteOff:
    """Агрегат по самому частому списанию за неделю."""

    product_name: str
    amount: Decimal
    count: int
    quantity: Decimal


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
    unit_uuids = list(snapshot.subs_by_unit.keys())

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
    day_writeoffs_task = _fetch_writeoffs(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=day_params,
    )
    week_writeoffs_task = _fetch_writeoffs(
        dodo_api=dodo_api,
        account_id=account_id,
        params_base=week_params,
    )
    revenue_result, day_writeoffs_result, week_writeoffs_result = await asyncio.gather(
        revenue_task,
        day_writeoffs_task,
        week_writeoffs_task,
    )

    if revenue_result[0] is None:
        return None, revenue_result[1], revenue_result[2]
    if day_writeoffs_result[0] is None:
        return None, day_writeoffs_result[1], day_writeoffs_result[2]
    if week_writeoffs_result[0] is None:
        return None, week_writeoffs_result[1], week_writeoffs_result[2]

    return _format_writeoff_message(
        unit_name=unit_name,
        date_label=date_label,
        revenue_total=revenue_result[0],
        day_writeoffs=day_writeoffs_result[0],
        weekly_frequent=_find_weekly_frequent_writeoff(week_writeoffs_result[0]),
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


async def _fetch_writeoffs(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
) -> tuple[Optional[List[WriteOffEntry]], Optional[str], Optional[int]]:
    """Загружает все списанные продукты за период."""
    skip = 0
    rows_result: List[WriteOffEntry] = []

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        response = await dodo_api.get(
            account_id,
            "accounting/write-offs/products",
            params=params,
        )
        if not response.ok:
            return None, response.error_html, response.status

        data = response.data if isinstance(response.data, dict) else {}
        rows = data.get("writeOffs")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            rows_result.append(
                WriteOffEntry(
                    # `productName` уже приходит из API в человекочитаемом виде
                    # и часто содержит фасовку (`1 шт`, `0,2 л`), поэтому ниже
                    # мы больше не пытаемся вручную собирать название.
                    product_name=str(row.get("productName") or "Неизвестный продукт").strip(),
                    reason=str(row.get("reason") or "Unknown").strip() or "Unknown",
                    amount=_to_decimal(row.get("pricePerPiece")) * _to_decimal(row.get("quantity")),
                    quantity=_to_decimal(row.get("quantity")),
                )
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    return rows_result, None, 200


def _format_writeoff_message(
    *,
    unit_name: str,
    date_label: str,
    revenue_total: Decimal,
    day_writeoffs: List[WriteOffEntry],
    weekly_frequent: Optional[WeeklyFrequentWriteOff],
) -> str:
    """Формирует итоговый daily-отчет по списаниям."""
    reasons_totals = _aggregate_reason_totals(day_writeoffs)
    most_expensive = _find_most_expensive_writeoff(day_writeoffs)

    lines = [
        f"🗑 <b>Списания продуктов за {date_label} в заведении {html.escape(unit_name)}</b>",
        f"Выручка: {int(revenue_total)}",
        "",
    ]

    if reasons_totals:
        lines.append("<b>По причинам списания:</b>")
        lines.extend(_format_reason_lines(reasons_totals, revenue_total))
    else:
        lines.append("<b>По причинам списания:</b>")
        lines.append("Списаний за день не найдено.")

    lines.append("")
    if most_expensive is not None:
        lines.append("<b>Самое дорогое списание:</b>")
        lines.append(f"{html.escape(most_expensive.product_name.strip())}:")
        lines.append(f"        Количество: {_format_quantity(most_expensive.quantity)} шт.")
        lines.append(f"        Стоимость: {int(most_expensive.amount)}")
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
        lines.append(f"        Стоимость: {int(weekly_frequent.amount)}")
        lines.append(f"        Количество: {_format_quantity(weekly_frequent.quantity)} шт.")
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
            str(int(amount)),
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


def _aggregate_reason_totals(rows: Iterable[WriteOffEntry]) -> Dict[str, Decimal]:
    """Суммирует стоимость списаний по причине."""
    totals: Dict[str, Decimal] = {}
    for row in rows:
        totals[row.reason] = totals.get(row.reason, Decimal("0")) + row.amount
    return totals


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
            )
            continue
        aggregated[row.product_name] = WeeklyFrequentWriteOff(
            product_name=current.product_name,
            amount=current.amount + row.amount,
            count=current.count + 1,
            # Показываем не только число событий, но и общий объем списанного
            # товара за 7 дней.
            quantity=current.quantity + row.quantity,
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


def _to_decimal(value: object) -> Decimal:
    """Безопасно приводит число к Decimal."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")

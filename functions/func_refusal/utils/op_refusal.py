"""Hourly job по отмененным заказам и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

import aiohttp
from dagster import OpExecutionContext

from database.models import Unit
from database.requests import (
    ReportSubscription,
    load_accounts_and_tokens_bulk,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_report_subscriptions,
)
from functions.shared.manual_report_runtime import html_to_log_text, split_text_for_telegram
from functions.shared.unit_time import resolve_unit_now
from messaging import build_payload

MAX_CONCURRENT_UNITS = 15
PAGE_SIZE = 900


@dataclass(slots=True)
class CancelledOrder:
    """Агрегированная отмененная продажа по одному orderId."""

    order_id: str
    sold_at: Optional[str]
    product_lines: List[str] = field(default_factory=list)
    total_price: Decimal = Decimal("0")


async def run_refusal(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку hourly refusal-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    subscriptions = await select_report_subscriptions(db, "refusal")
    if not subscriptions:
        context.log.info("refusal_op: нет подписок")
        return

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
                    context.log.warning("refusal_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_refusal_report(
                        dodo_api=dodo_api,
                        unit=unit,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                    )
                    if report_text is not None:
                        break
                    if last_error_html and last_error_status in {401, 403}:
                        context.log.warning(
                            "refusal_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            html_to_log_text(last_error_html),
                        )

                if report_text is None:
                    if last_error_html:
                        if last_error_status in {429, 500}:
                            context.log.error(
                                "refusal_op: не удалось собрать отчет для unit %s после retry: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                        elif last_error_status not in {401, 403}:
                            context.log.error(
                                "refusal_op: не удалось собрать отчет для unit %s: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                    return

                if not report_text:
                    return

                for text_chunk in split_text_for_telegram(report_text, prefer_paragraph_split=True):
                    for sub in subscribers:
                        payload = build_payload(
                            chat_id=sub.chat_id,
                            topic_id=sub.topic_id,
                            post="refusal",
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


async def _build_refusal_report(
    *,
    dodo_api,
    unit: Optional[Unit],
    unit_uuid: str,
    unit_name: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end = _build_report_window(unit)
    cancelled_orders, error_html, error_status = await _fetch_cancelled_orders(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        dt_start=dt_start,
        dt_end=dt_end,
    )
    if cancelled_orders is None:
        return None, error_html, error_status
    if not cancelled_orders:
        return "", None, 200

    return _format_refusal_message(
        unit_name=unit_name,
        cancelled_orders=cancelled_orders,
        dt_start=dt_start,
        dt_end=dt_end,
    ), None, 200


async def _fetch_cancelled_orders(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[Optional[List[CancelledOrder]], Optional[str], Optional[int]]:
    skip = 0
    by_order_id: Dict[str, CancelledOrder] = {}

    while True:
        result = await dodo_api.get(
            account_id,
            "accounting/cancelled-sales",
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
        rows = data.get("cancelledSales")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            order_id = str(row.get("orderId") or "").strip()
            if not order_id:
                continue

            order = by_order_id.get(order_id)
            if order is None:
                order = CancelledOrder(
                    order_id=order_id,
                    sold_at=_extract_time(row.get("soldAt")),
                )
                by_order_id[order_id] = order

            product_name = str(row.get("productName") or "").strip()
            if product_name:
                order.product_lines.append(product_name)
            order.total_price += _to_decimal(row.get("price"))

        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    cancelled_orders = sorted(
        by_order_id.values(),
        key=lambda item: (item.sold_at or "", item.order_id),
    )
    return cancelled_orders, None, 200


def _format_refusal_message(
    *,
    unit_name: str,
    cancelled_orders: List[CancelledOrder],
    dt_start: datetime,
    dt_end: datetime,
) -> str:
    """Собирает итоговый hourly-отчет по отказам для одной точки."""
    lines = [
        f"⛔ <b>Отмененные заказы в {html.escape(unit_name)}</b>",
        f"Период: {dt_start.strftime('%H:%M')} - {dt_end.strftime('%H:%M')}",
        "",
    ]
    for order in cancelled_orders:
        lines.append(f"Время заказа: {order.sold_at or '-'}")
        lines.append(f"Номер заказа: {html.escape(order.order_id)}")
        lines.append("Продукты:")
        if order.product_lines:
            for product_name in order.product_lines:
                lines.append(f"- {html.escape(product_name)}")
        else:
            lines.append("- Не удалось определить состав")
        lines.append(f"Стоимость: {int(order.total_price)} 💵")
        lines.append("")
    lines.append("#отказы")
    return "\n".join(lines).strip()


def _build_report_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Строит окно за предыдущий завершенный локальный час точки."""
    # Берем только уже завершенный час по локальному времени заведения,
    # чтобы отчет не плавал внутри текущего часа.
    local_now = resolve_unit_now(unit)
    dt_end = local_now.replace(minute=0, second=0, microsecond=0).replace(tzinfo=None)
    dt_start = dt_end - timedelta(hours=1)
    return dt_start, dt_end


def _extract_time(value: object) -> Optional[str]:
    """Достает HH:MM:SS из ISO datetime строки."""
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime("%H:%M:%S")
    except ValueError:
        if "T" in value:
            return value.split("T", 1)[1].split(".", 1)[0]
    return None


def _to_decimal(value: object) -> Decimal:
    """Безопасно преобразует значение цены к Decimal."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")

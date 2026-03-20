"""Hourly job по отмененным заказам и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

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
from functions.shared.unit_time import build_previous_local_hour_window

MAX_CONCURRENT_UNITS = 15
PAGE_SIZE = 900


@dataclass(slots=True)
class CancelledOrder:
    """Агрегированная отмененная продажа по одному orderId."""

    order_id: str
    sold_at: Optional[str]
    product_lines: List[str] = field(default_factory=list)
    total_price: Decimal = Decimal("0")
    type_order: Optional[str] = None


async def run_refusal(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку hourly refusal-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "refusal"),
    )
    if not snapshot.subscriptions:
        context.log.info("refusal_op: нет подписок")
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
                    context.log.warning("refusal_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_refusal_report(
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
                        log_prefix="refusal_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="refusal_op",
                        unit_uuid=unit_uuid,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )
                    return

                if not report_text:
                    return

                await publish_report_to_subscribers(
                    rabbitmq,
                    publish_lock=publish_lock,
                    subscribers=subscribers,
                    post="refusal",
                    text=report_text,
                    unit_uuid=unit_uuid,
                    prefer_paragraph_split=True,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


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

            type_order = str(row.get("salesChannel") or "").strip()
            if type_order:
                if type_order == "Delivery":
                    type_order = "Доставка"
                else:
                    type_order = "Ресторан"
                order.type_order = type_order

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
        lines.append(f"Тип заказа: {html.escape(order.type_order or '-')}")
        # lines.append(f"Номер заказа: {html.escape(order.order_id)}")
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
    return build_previous_local_hour_window(unit)


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

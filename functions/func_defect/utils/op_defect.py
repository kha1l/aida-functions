"""Hourly job по браку и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
MIN_DEFECTS_PER_HOUR = 3


@dataclass(slots=True)
class DefectItem:
    """Одна запись брака из Dodo API."""

    product_name: str
    sold_at: Optional[str]
    added_ingredients: List[str] = field(default_factory=list)
    pizza_halves: List[str] = field(default_factory=list)


async def run_defect(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку hourly-отчетов по браку."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "defect"),
    )
    if not snapshot.subscriptions:
        context.log.info("defect_op: нет подписок")
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
                    context.log.warning("defect_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_defect_report(
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
                        log_prefix="defect_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="defect_op",
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
                    post="defect",
                    text=report_text,
                    unit_uuid=unit_uuid,
                    prefer_paragraph_split=True,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def _build_defect_report(
    *,
    dodo_api,
    unit: Optional[Unit],
    unit_uuid: str,
    unit_name: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end = _build_report_window(unit)
    defects, error_html, error_status = await _fetch_defects(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        dt_start=dt_start,
        dt_end=dt_end,
    )
    if defects is None:
        return None, error_html, error_status
    if len(defects) < MIN_DEFECTS_PER_HOUR:
        return "", None, 200

    return _format_defect_message(
        unit_name=unit_name,
        defects=defects,
        dt_start=dt_start,
        dt_end=dt_end,
    ), None, 200


async def _fetch_defects(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[Optional[List[DefectItem]], Optional[str], Optional[int]]:
    skip = 0
    defects: List[DefectItem] = []

    while True:
        result = await dodo_api.get(
            account_id,
            "accounting/defective-products",
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
        rows = data.get("defects")
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            defects.append(
                DefectItem(
                    product_name=str(row.get("productName") or "Неизвестный продукт").strip(),
                    sold_at=_extract_time(row.get("soldAtLocal") or row.get("soldAt")),
                    added_ingredients=_extract_nested_names(row.get("addedIngredients")),
                    pizza_halves=_extract_nested_names(row.get("pizzaHalves")),
                )
            )

        if data.get("isEndOfListReached") or len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    defects.sort(key=lambda item: (item.sold_at or "", item.product_name))
    return defects, None, 200


def _format_defect_message(
    *,
    unit_name: str,
    defects: List[DefectItem],
    dt_start: datetime,
    dt_end: datetime,
) -> str:
    """Собирает итоговый hourly-отчет по браку для одной точки."""
    lines = [
        f"🧪 <b>Брак в {html.escape(unit_name)}</b>",
        f"Период: {dt_start.strftime('%H:%M')} - {dt_end.strftime('%H:%M')}",
        f"Брака за час: {len(defects)}",
        "",
    ]

    for defect in defects:
        lines.append(f"Наименование продукта: {html.escape(defect.product_name)}")
        for ingredient_name in defect.added_ingredients:
            lines.append(f"    Добавленный ингредиент: {html.escape(ingredient_name)}")
        for half_name in defect.pizza_halves:
            lines.append(f"    Пицца-половинка: {html.escape(half_name)}")
        lines.append("")

    lines.append("#брак")
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


def _extract_nested_names(value: object) -> List[str]:
    """Извлекает человекочитаемые названия вложенных сущностей из массива объектов."""
    if not isinstance(value, list):
        return []

    names: List[str] = []
    for item in value:
        name = _extract_name(item)
        if not name:
            continue
        names.append(name)
    return names


def _extract_name(value: object) -> Optional[str]:
    """Пытается достать название продукта/ингредиента из словаря Dodo API."""
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if not isinstance(value, dict):
        return None

    for key in (
        "productName",
        "name",
        "ingredientName",
        "addedIngredientName",
        "pizzaHalfName",
    ):
        text = str(value.get(key) or "").strip()
        if text:
            return text
    return None

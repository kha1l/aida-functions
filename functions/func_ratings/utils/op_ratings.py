"""Ежедневный job по рейтингам РКО/РС."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

from dagster import OpExecutionContext

from config import ratings_api_settings
from database.models import Unit
from database.requests import select_report_subscriptions
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)

PAGE_SIZE = 100
MAX_CONCURRENT_UNITS = 15
MSK_TZ = ZoneInfo("Europe/Moscow")
PUBLISH_STATUS_LABELS = {
    "Calculated": "Предварительный результат",
    "Published": "Финальный результат",
}


@dataclass(frozen=True, slots=True)
class RatingEntry:
    """Нормализованная запись рейтинга из control API."""

    code: str
    period_label: str
    unit_name: str
    published_at: datetime
    publish_status: str
    rate: Decimal | None
    avg_rate: Decimal | None
    common_ratings_position: int | None


async def run_ratings(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных отчетов по рейтингам."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "ratings"),
    )
    if not snapshot.subscriptions:
        context.log.info("ratings_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()

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
                    context.log.warning("ratings_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_ratings_report(
                        dodo_api=dodo_api,
                        unit=unit,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                    )
                    if last_error_status == 200 and last_error_html is None:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="ratings_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="ratings_op",
                        unit_uuid=unit_uuid,
                        error_html=last_error_html,
                        error_status=last_error_status,
                        empty_message="нет данных по рейтингам",
                    )
                    return

                if not report_text:
                    return

                await publish_report_to_subscribers(
                    rabbitmq,
                    publish_lock=publish_lock,
                    subscribers=subscribers,
                    post="ratings",
                    text=report_text,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in snapshot.subs_by_unit.keys()))


async def _build_ratings_report(
    *,
    dodo_api,
    unit: Optional[Unit],
    unit_uuid: str,
    unit_name: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Собирает отчет по рейтингам для одной точки."""
    cfg = ratings_api_settings()
    standard_task = _fetch_rating_entry(
        dodo_api=dodo_api,
        account_id=account_id,
        url=cfg.standards_url,
        unit_uuid=unit_uuid,
        fallback_unit_name=unit_name,
        code="РС",
        period_label="Средний рейтинг за 3 месяца",
    )
    customer_task = _fetch_rating_entry(
        dodo_api=dodo_api,
        account_id=account_id,
        url=cfg.customer_experience_url,
        unit_uuid=unit_uuid,
        fallback_unit_name=unit_name,
        code="РКО",
        period_label="Средний рейтинг за 12 недель",
    )
    standard_result, customer_result = await asyncio.gather(standard_task, customer_task)

    for entry, error_html, status in (customer_result, standard_result):
        if error_html is not None or status != 200:
            return None, error_html, status

    recent_entries = [
        entry
        for entry in (customer_result[0], standard_result[0])
        if entry is not None and _is_recent_rating(entry.published_at, hours=cfg.recent_hours)
    ]
    if not recent_entries:
        return None, None, 200

    return _format_ratings_message(recent_entries), None, 200


async def _fetch_rating_entry(
    *,
    dodo_api,
    account_id: int,
    url: str,
    unit_uuid: str,
    fallback_unit_name: str,
    code: str,
    period_label: str,
) -> tuple[Optional[RatingEntry], Optional[str], Optional[int]]:
    """Получает актуальную запись рейтинга для выбранной точки."""
    rows, error_html, status = await _fetch_rating_rows(
        dodo_api=dodo_api,
        account_id=account_id,
        url=url,
        unit_uuid=unit_uuid,
    )
    if rows is None:
        return None, error_html, status

    row = _pick_rating_row(rows, unit_uuid)
    if row is None:
        return None, None, 200

    entry = _normalize_rating_row(
        row,
        code=code,
        period_label=period_label,
        fallback_unit_name=fallback_unit_name,
    )
    return entry, None, 200


async def _fetch_rating_rows(
    *,
    dodo_api,
    account_id: int,
    url: str,
    unit_uuid: str,
) -> tuple[Optional[list[dict[str, Any]]], Optional[str], Optional[int]]:
    """Постранично читает ratings-эндпоинт по одной точке."""
    rows: list[dict[str, Any]] = []
    skip = 0

    while True:
        result = await dodo_api.get(
            account_id,
            url,
            params={
                "units": unit_uuid,
                "take": PAGE_SIZE,
                "skip": skip,
            },
        )
        if not result.ok:
            return None, result.error_html, result.status

        data = result.data
        batch = _extract_rows(data)
        rows.extend(batch)

        if _is_end_of_list(data, batch_count=len(batch)):
            break
        if not batch:
            break
        skip += len(batch)

    return rows, None, 200


def _extract_rows(data: Any) -> list[dict[str, Any]]:
    """Достает список rating-объектов из разных возможных форматов ответа."""
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []

    # Control ratings API возвращает корневые метаданные публикации
    # и список точек в `unitRates`. Склеиваем их в плоские записи,
    # чтобы дальше парсер работал одинаково для обеих ручек.
    unit_rates = data.get("unitRates")
    if isinstance(unit_rates, list):
        shared = {
            "periodFrom": data.get("periodFrom"),
            "periodTo": data.get("periodTo"),
            "publishStatus": data.get("publishStatus"),
            "publishedAt": data.get("publishedAt"),
        }
        rows: list[dict[str, Any]] = []
        for item in unit_rates:
            if isinstance(item, dict):
                rows.append({**shared, **item})
        return rows

    for key in (
        "ratings",
        "standardRatings",
        "customerExperienceRatings",
        "items",
        "results",
        "data",
    ):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    if _looks_like_rating_row(data):
        return [data]
    return []


def _is_end_of_list(data: Any, *, batch_count: int) -> bool:
    """Определяет, что постраничный список дочитан до конца."""
    if isinstance(data, dict):
        for key in ("isEndOfListReached", "isLastPage", "lastPage", "endOfListReached"):
            value = data.get(key)
            if isinstance(value, bool):
                return value
    return batch_count < PAGE_SIZE


def _looks_like_rating_row(row: dict[str, Any]) -> bool:
    return any(key in row for key in ("publishStatus", "publishedAt", "avgRate", "commonRatingsPosition"))


def _pick_rating_row(rows: Sequence[dict[str, Any]], unit_uuid: str) -> Optional[dict[str, Any]]:
    """Выбирает запись для нужной точки; при точном совпадении отдает ее, иначе берет первую."""
    if not rows:
        return None

    normalized_unit_uuid = unit_uuid.strip().lower()
    for row in rows:
        row_unit_id = _extract_unit_id(row)
        if row_unit_id and row_unit_id.strip().lower() == normalized_unit_uuid:
            return row
    return rows[0]


def _extract_unit_id(row: dict[str, Any]) -> Optional[str]:
    """Пытается достать unit_uuid из rating-объекта."""
    for key in ("unitId", "unitUuid", "pizzeriaId", "restaurantId", "unit"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for nested_key in ("id", "uuid", "unitId"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, str) and nested_value.strip():
                    return nested_value.strip()
    return None


def _normalize_rating_row(
    row: dict[str, Any],
    *,
    code: str,
    period_label: str,
    fallback_unit_name: str,
) -> RatingEntry:
    """Приводит сырую запись control API к единому DTO."""
    published_at = _parse_datetime(row.get("publishedAt")) or datetime.now(timezone.utc)
    unit_name = _extract_unit_name(row) or fallback_unit_name
    return RatingEntry(
        code=code,
        period_label=period_label,
        unit_name=unit_name,
        published_at=published_at,
        publish_status=str(row.get("publishStatus") or ""),
        rate=_to_decimal(row.get("rate")),
        avg_rate=_to_decimal(row.get("avgRate")),
        common_ratings_position=_to_int(row.get("commonRatingsPosition")),
    )


def _extract_unit_name(row: dict[str, Any]) -> Optional[str]:
    """Пытается вытащить название точки из разных форматов ответа."""
    for key in ("unitName", "name", "pizzeriaName", "restaurantName"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("unit", "pizzeria", "restaurant"):
        value = row.get(key)
        if isinstance(value, dict):
            for nested_key in ("name", "title"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, str) and nested_value.strip():
                    return nested_value.strip()
    return None


def _is_recent_rating(published_at: datetime, *, hours: int) -> bool:
    """Проверяет, что рейтинг опубликован за последние N часов."""
    now_utc = datetime.now(timezone.utc)
    published_utc = published_at.astimezone(timezone.utc)
    return published_utc >= now_utc - timedelta(hours=hours)


def _format_ratings_message(entries: Sequence[RatingEntry]) -> str:
    """Форматирует итоговый текст отчета по рейтингам."""
    blocks = [_format_rating_block(entry) for entry in entries]
    blocks.append("#рейтинги")
    return "\n\n".join(blocks)


def _format_rating_block(entry: RatingEntry) -> str:
    """Форматирует один блок по конкретному рейтингу."""
    published_label = entry.published_at.astimezone(MSK_TZ).strftime("%d.%m %H:%M")
    status_label = PUBLISH_STATUS_LABELS.get(entry.publish_status, entry.publish_status or "-")
    lines = [
        f"📈 <b>Изменение в {entry.code} {html.escape(entry.unit_name)}</b> {published_label}",
        f"Статус рейтинга: {html.escape(status_label)}",
    ]

    if entry.rate is not None:
        lines.append(f"Оценка: {_format_decimal(entry.rate)} {_rating_color(entry.rate)}")

    if entry.avg_rate is not None:
        lines.append(f"{entry.period_label}: {_format_decimal(entry.avg_rate)}")
    else:
        lines.append(f"{entry.period_label}: -")

    lines.append(
        f"Место в рейтинге: {entry.common_ratings_position if entry.common_ratings_position is not None else '-'}"
    )

    prescription_text = _prescription_text(entry.avg_rate, entry.publish_status)
    if prescription_text:
        lines.append(prescription_text)

    return "\n".join(lines)


def _prescription_text(avg_rate: Decimal | None, publish_status: str) -> Optional[str]:
    """Возвращает текст по риску предписания."""
    if avg_rate is None:
        return None
    if avg_rate < Decimal("80") and publish_status == "Published":
        return "Предписание! 🔴"
    if avg_rate < Decimal("85"):
        return "Возможно предписание! 🟠"
    return None


def _rating_color(value: Decimal) -> str:
    """Подбирает цвет-эмодзи для оценки рейтинга."""
    if value < Decimal("75"):
        return "🔴"
    if value < Decimal("85"):
        return "🟠"
    if value < Decimal("95"):
        return "🟡"
    return "🟢"


def _format_decimal(value: Decimal) -> str:
    """Форматирует decimal до двух знаков после запятой."""
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:.2f}".replace(".", ",")


def _to_decimal(value: Any) -> Decimal | None:
    """Безопасно приводит число к Decimal."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    """Безопасно приводит значение к int."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    """Парсит publishedAt в aware datetime."""
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed

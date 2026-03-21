"""Сбор manual-отчета по трекингу для нескольких выбранных заведений."""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from dagster import OpExecutionContext

from database.models import Unit
from database.requests import (
    load_accounts_and_tokens_bulk,
    load_chat_languages,
    load_units_by_uuids,
    select_account_ids_for_units,
)
from functions.shared.manual_report_runtime import html_to_log_text, split_text_for_telegram
from functions.shared.report_job_runtime import log_auth_error, report_job_runtime
from functions.shared.report_text_i18n import translate_report_text
from functions.shared.unit_time import resolve_unit_today
from messaging import build_payload


TOP_LIMIT = 5


@dataclass(frozen=True, slots=True)
class TrackingEntry:
    """Одна строка трекинг-метрик по продукту."""

    product_name: str
    products_count: int
    total_duration: str
    total_duration_seconds: int


async def run_tracking(context: OpExecutionContext) -> None:
    """Собирает единый трекинг-отчет по выбранным пользователем заведениям."""
    dodo_api = context.resources.dodo_api
    rabbitmq = context.resources.rabbitmq
    db = context.resources.db

    unit_uuids = _normalize_unit_uuids(context.op_config.get("unit_uuids"))
    if not unit_uuids:
        context.log.warning("tracking_op: не переданы unit_uuids")
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
        report_blocks: List[str] = []
        sorted_unit_uuids = sorted(
            unit_uuids,
            key=lambda unit_uuid: _unit_name(units.get(unit_uuid), unit_uuid).lower(),
        )

        for unit_uuid in sorted_unit_uuids:
            unit = units.get(unit_uuid)
            candidate_accounts = candidate_accounts_by_unit.get(unit_uuid, [])
            if not candidate_accounts:
                context.log.warning("tracking_op: нет токенов для unit %s", unit_uuid)
                continue

            block_text: Optional[str] = None
            last_error_html: Optional[str] = None
            last_error_status: Optional[int] = None

            for account_id in candidate_accounts:
                block_text, last_error_html, last_error_status = await _build_unit_tracking_block(
                    dodo_api=dodo_api,
                    unit=unit,
                    unit_uuid=unit_uuid,
                    account_id=account_id,
                )
                if block_text is not None and last_error_status == 200:
                    break

                log_auth_error(
                    context,
                    log_prefix="tracking_op",
                    unit_uuid=unit_uuid,
                    account_id=account_id,
                    error_html=last_error_html,
                    error_status=last_error_status,
                )

            if block_text:
                report_blocks.append(block_text)
                continue

            if last_error_html and last_error_status not in {401, 403}:
                context.log.error(
                    "tracking_op: не удалось собрать трекинг для unit %s: %s",
                    unit_uuid,
                    html_to_log_text(last_error_html),
                )

        if not report_blocks:
            context.log.warning("tracking_op: не удалось собрать ни одного блока отчета")
            return

        report_text = translate_report_text(
            "tracking",
            report_language,
            "\n\n".join(report_blocks + ["#трекинг"]),
        )
        for text_chunk in split_text_for_telegram(
            report_text,
            prefer_paragraph_split=True,
        ):
            payload = build_payload(
                chat_id=chat_id,
                topic_id=topic_id,
                post="tracking",
                text=text_chunk,
            )
            await rabbitmq.publish_async(payload)


async def _build_unit_tracking_block(
    *,
    dodo_api,
    unit: Optional[Unit],
    unit_uuid: str,
    account_id: int,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Собирает трекинг-блок для одного заведения."""
    report_date = resolve_unit_today(unit).strftime("%Y-%m-%d")
    result = await dodo_api.get(
        account_id,
        "production/tracking-metrics/summary",
        params={
            "units": unit_uuid,
            "fromDate": report_date,
            "toDate": report_date,
        },
    )
    if not result.ok:
        return None, result.error_html, result.status

    data = result.data if isinstance(result.data, dict) else {}
    entries = _collect_tracking_entries(data, unit_uuid=unit_uuid)
    return _format_tracking_block(
        unit_name=_unit_name(unit, unit_uuid),
        report_date=report_date,
        entries=entries,
    ), None, 200


def _collect_tracking_entries(
    data: Dict[str, object],
    *,
    unit_uuid: str,
) -> List[TrackingEntry]:
    """Нормализует результат tracking summary в список строк по продуктам."""
    entries: List[TrackingEntry] = []

    for item in data.get("result", []) or []:
        if not isinstance(item, dict):
            continue

        item_unit_uuid = str(item.get("unitId") or "").strip().lower()
        if item_unit_uuid and item_unit_uuid != unit_uuid.lower():
            continue

        total_duration_seconds = _sum_station_durations(item.get("stations"))
        if total_duration_seconds is None:
            continue

        entries.append(
            TrackingEntry(
                product_name=str(item.get("productName") or "-").strip() or "-",
                products_count=_to_int(item.get("productsCount")) or 0,
                total_duration=_format_seconds_to_hhmmss(total_duration_seconds),
                total_duration_seconds=total_duration_seconds,
            )
        )

    return entries


def _format_tracking_block(
    *,
    unit_name: str,
    report_date: str,
    entries: List[TrackingEntry],
) -> str:
    """Собирает человекочитаемый блок трекинга по одной точке."""
    date_label = _format_date(report_date)
    lines = [
        f"🛰 <b>Трекинг за {date_label} в заведении {html.escape(unit_name)}</b>",
    ]

    if not entries:
        lines.append("Данных по трекингу за текущий день пока нет.")
        return "\n".join(lines).rstrip()

    top_by_count = sorted(
        entries,
        key=lambda item: (-item.products_count, item.total_duration_seconds, item.product_name.lower()),
    )[:TOP_LIMIT]
    # Во втором блоке специально поднимаем наверх самые долгие продукты.
    top_by_duration = sorted(
        entries,
        key=lambda item: (-item.total_duration_seconds, -item.products_count, item.product_name.lower()),
    )[:TOP_LIMIT]

    lines.append("⬆️ Топ-5 по количеству:")
    lines.extend(_format_tracking_lines(top_by_count))
    lines.append("")
    lines.append("⬇️ Tоп-5 по самому долгому времени:")
    lines.extend(_format_tracking_lines(top_by_duration))
    return "\n".join(lines).rstrip()


def _format_tracking_lines(entries: Iterable[TrackingEntry]) -> List[str]:
    """Форматирует список продуктов в строки отчета."""
    lines: List[str] = []
    for index, entry in enumerate(entries, start=1):
        # Формат делаем двухстрочным: так длинные названия читаются лучше,
        # чем в одну строку с несколькими колонками.
        lines.append(f"{index}. {html.escape(entry.product_name)}:")
        lines.append(f"        Приготовлено: {entry.products_count} шт.")
        lines.append(f"        Среднее время: {entry.total_duration}")
    return lines


def _normalize_unit_uuids(value: object) -> List[str]:
    """Убирает дубли и пустые значения из списка заведений."""
    if not isinstance(value, list):
        return []

    seen: set[str] = set()
    result: List[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _unit_name(unit: Optional[Unit], unit_uuid: str) -> str:
    """Возвращает имя заведения или uuid, если запись не найдена."""
    return unit.name if unit and unit.name else unit_uuid


def _parse_hhmmss_to_seconds(value: object) -> Optional[int]:
    """Переводит строку вида HH:mm:ss в секунды."""
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if hours < 0 or minutes < 0 or seconds < 0:
        return None
    return hours * 3600 + minutes * 60 + seconds


def _sum_station_durations(value: object) -> Optional[int]:
    """Суммирует `avgDuration` по всем станциям продукта."""
    if not isinstance(value, list):
        return None

    total_seconds = 0
    has_station_duration = False
    for station in value:
        if not isinstance(station, dict):
            continue
        duration_seconds = _parse_hhmmss_to_seconds(station.get("avgDuration"))
        if duration_seconds is None:
            continue
        has_station_duration = True
        total_seconds += duration_seconds

    if not has_station_duration:
        return None
    return total_seconds


def _format_seconds_to_hhmmss(value: int) -> str:
    """Преобразует секунды в строку `HH:MM:SS`."""
    hours, remainder = divmod(value, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_date(value: str) -> str:
    """Переводит YYYY-MM-DD в DD.MM."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%d.%m")
    except Exception:  # noqa: BLE001
        return value


def _to_int(value: object) -> Optional[int]:
    """Безопасно приводит значение к int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

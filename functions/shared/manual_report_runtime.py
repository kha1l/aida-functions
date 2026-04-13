"""Общий runtime для manual-отчетов в `func_couriers` и `func_stationary`."""

from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_chat_languages, load_chat_platforms, load_units_by_uuids
from functions.shared.report_text_i18n import translate_report_text
from functions.shared.webapp_results import store_webapp_report_result
from messaging import build_payload


TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000
REPORT_TITLES = {
    "certificates": "Сертификаты",
    "later": "Опоздания/прогулы",
    "sectors": "Сектора доставки",
    "longers": "Долгие доставки",
    "later_rest": "Долгие заказы",
    "assembly_rest": "Долгая сборка заказа",
    "drink_rest": "Долгие напитки",
}


@dataclass(frozen=True, slots=True)
class ManualUnitReportRequest:
    """Контекст запуска manual-отчета по одной точке."""

    unit_uuid: str
    unit_name: str
    unit_timezone: Optional[int]
    account_id: int
    account_properties: Optional[str]
    chat_id: int
    topic_id: Optional[int]
    language: str
    platform: str
    message_date: Optional[str]
    op_config: dict[str, object]


ManualUnitReportBuilder = Callable[
    [object, ManualUnitReportRequest],
    Awaitable[tuple[Optional[str], Optional[str], Optional[int]]],
]


async def run_manual_unit_report(
    context: OpExecutionContext,
    *,
    post: str,
    log_prefix: str,
    build_report: ManualUnitReportBuilder,
    prefer_paragraph_split: bool = False,
) -> None:
    """Общий orchestrator для manual-отчетов по одной точке."""

    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    op_config = context.op_config
    unit_uuid = op_config["unit_uuid"]
    account_id = op_config["account_id"]
    chat_id = op_config["chat_id"]
    topic_id = op_config.get("topic_id")
    message_date = op_config.get("message_date")
    result_request_id = str(op_config.get("result_request_id") or "")

    # Manual-job всегда работает с одной точкой, но используем bulk helper для единообразия.
    units = await load_units_by_uuids(db, [unit_uuid])
    unit = units.get(unit_uuid)
    unit_name = unit.name if unit and unit.name else unit_uuid
    unit_timezone = unit.timezone if unit else None

    # Сразу прогреваем dodo_api кеш, чтобы дальше не ходить в БД повторно.
    account_auth_map = await load_accounts_and_tokens_bulk(db, {account_id})
    dodo_api.prime_account_token_cache(account_auth_map)
    account, _ = account_auth_map.get(account_id, (None, None))
    if result_request_id:
        language = "ru"
        platform = "webapp"
    else:
        chat_languages = await load_chat_languages(
            db,
            [chat_id],
            fallback_account_ids_by_chat={int(chat_id): int(account_id)},
        )
        chat_platforms = await load_chat_platforms(db, [chat_id])
        language = chat_languages.get(chat_id, "ru")
        platform = chat_platforms.get(chat_id, "telegram")

    request = ManualUnitReportRequest(
        unit_uuid=unit_uuid,
        unit_name=unit_name,
        unit_timezone=unit_timezone,
        account_id=account_id,
        account_properties=account.properties if account else None,
        chat_id=chat_id,
        topic_id=topic_id,
        language=language,
        platform=platform,
        message_date=message_date,
        op_config=dict(op_config),
    )

    request_semaphore = None
    if getattr(dodo_api, "max_concurrent_requests", None):
        request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))

    dodo_timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)
    async with aiohttp.ClientSession(timeout=dodo_timeout, trust_env=True) as dodo_session:
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=request_semaphore,
        )
        if result_request_id:
            try:
                await _build_and_store_webapp_report(
                    db=db,
                    request_id=result_request_id,
                    post=post,
                    build_report=build_report,
                    dodo_api=dodo_api,
                    request=request,
                )
            except Exception as exc:
                await _store_webapp_error(
                    db=db,
                    request_id=result_request_id,
                    post=post,
                    request=request,
                    error=str(exc),
                )
                raise
            finally:
                dodo_api.reset_runtime()
                dodo_api.clear_account_token_cache()
            return

        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        try:
            report_text, last_error_html, last_error_status = await build_report(
                dodo_api=dodo_api,
                request=request,
            )

            if not report_text:
                log_report_failure(
                    context=context,
                    log_prefix=log_prefix,
                    request=request,
                    error_html=last_error_html,
                    error_status=last_error_status,
                )
                return

            report_text = translate_report_text(post, request.language, report_text)

            # Режем только перед отправкой в RabbitMQ, чтобы builder возвращал единый текст отчета.
            for text_chunk in split_text_for_telegram(
                report_text,
                prefer_paragraph_split=prefer_paragraph_split,
            ):
                payload = build_payload(
                    chat_id=request.chat_id,
                    topic_id=request.topic_id,
                    post=post,
                    text=text_chunk,
                    unit_uuid=request.unit_uuid,
                )
                await rabbitmq.publish_async(payload, platform=request.platform)
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def _build_and_store_webapp_report(
    *,
    db,
    request_id: str,
    post: str,
    build_report: ManualUnitReportBuilder,
    dodo_api,
    request: ManualUnitReportRequest,
) -> None:
    report_text, last_error_html, last_error_status = await build_report(
        dodo_api=dodo_api,
        request=request,
    )
    dt_start, dt_end, date_label = build_report_window(
        request.message_date,
        request.unit_timezone,
    )

    if not report_text:
        error = html_to_log_text(last_error_html or "Не удалось собрать отчет")
        await store_webapp_report_result(
            db=db,
            request_id=request_id,
            status="error",
            error=error,
            payload={
                "ok": False,
                "post": post,
                "request_id": request_id,
                "unit_uuid": request.unit_uuid,
                "unit_name": request.unit_name,
                "date_label": date_label,
                "window": {"from": dt_start.isoformat(), "to": dt_end.isoformat()},
                "error": error,
                "error_status": last_error_status,
            },
        )
        return

    report_text = translate_report_text(post, request.language, report_text)
    await store_webapp_report_result(
        db=db,
        request_id=request_id,
        status="done",
        error=None,
        payload={
            "ok": True,
            "post": post,
            "request_id": request_id,
            "unit_uuid": request.unit_uuid,
            "unit_name": request.unit_name,
            "date_label": date_label,
            "window": {"from": dt_start.isoformat(), "to": dt_end.isoformat()},
            "report_title": REPORT_TITLES.get(post, post),
            "report_text": report_text,
            "report_html": telegram_html_to_web_html(report_text),
        },
    )


async def _store_webapp_error(
    *,
    db,
    request_id: str,
    post: str,
    request: ManualUnitReportRequest,
    error: str,
) -> None:
    await store_webapp_report_result(
        db=db,
        request_id=request_id,
        status="error",
        error=error,
        payload={
            "ok": False,
            "post": post,
            "request_id": request_id,
            "unit_uuid": request.unit_uuid,
            "unit_name": request.unit_name,
            "error": error,
        },
    )


def telegram_html_to_web_html(value: str) -> str:
    """Безопасно переносит простой Telegram HTML в WebApp-страницу."""
    escaped = html.escape(value)
    allowed_tags = ("b", "strong", "i", "em", "u", "s", "code")
    for tag in allowed_tags:
        escaped = escaped.replace(f"&lt;{tag}&gt;", f"<{tag}>")
        escaped = escaped.replace(f"&lt;/{tag}&gt;", f"</{tag}>")
    return escaped.replace("\n", "<br>")


def build_report_window(
    message_date: Optional[str],
    unit_timezone: Optional[int] = None,
) -> tuple[datetime, datetime, str]:
    reference_dt = parse_reference_datetime(message_date, unit_timezone=unit_timezone)
    if reference_dt is None:
        now_utc = datetime.now(timezone.utc)
        if unit_timezone is not None:
            tz = timezone(timedelta(hours=unit_timezone))
            reference_dt = now_utc.astimezone(tz).replace(tzinfo=None)
        else:
            reference_dt = now_utc.replace(tzinfo=None)
    dt_end = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end, dt_start.strftime("%d.%m")


def parse_reference_datetime(
    value: object,
    unit_timezone: Optional[int] = None,
) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        if unit_timezone is not None:
            tz = timezone(timedelta(hours=unit_timezone))
            return parsed.astimezone(tz).replace(tzinfo=None)
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def format_clock(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.strftime("%H:%M:%S")


def split_text_for_telegram(
    text: str,
    *,
    prefer_paragraph_split: bool = False,
) -> list[str]:
    """Режет длинный текст под лимит Telegram, по возможности сохраняя структуру блоков."""
    if len(text) <= TELEGRAM_MESSAGE_LIMIT:
        return [text]

    chunks: list[str] = []
    current = text
    while len(current) > SAFE_TELEGRAM_CHUNK:
        split_at = -1
        if prefer_paragraph_split:
            split_at = current.rfind("\n\n", 0, SAFE_TELEGRAM_CHUNK)
        if split_at <= 0:
            split_at = current.rfind("\n", 0, SAFE_TELEGRAM_CHUNK)
        if split_at <= 0:
            split_at = SAFE_TELEGRAM_CHUNK
        chunks.append(current[:split_at].rstrip())
        current = current[split_at:].lstrip()
    if current:
        chunks.append(current)
    return chunks


def html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def to_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def log_report_failure(
    *,
    context: OpExecutionContext,
    log_prefix: str,
    request: ManualUnitReportRequest,
    error_html: Optional[str],
    error_status: Optional[int],
) -> None:
    """Логирует финальную ошибку manual-отчета в одном месте и в едином формате."""
    if error_html:
        if error_status in {401, 403}:
            context.log.warning(
                "%s: ошибка Dodo API для unit %s, account_id=%s: %s",
                log_prefix,
                request.unit_uuid,
                request.account_id,
                html_to_log_text(error_html),
            )
        elif error_status in {429, 500}:
            context.log.error(
                "%s: не удалось собрать отчет для unit %s после retry: %s",
                log_prefix,
                request.unit_uuid,
                html_to_log_text(error_html),
            )
        else:
            context.log.error(
                "%s: не удалось собрать отчет для unit %s: %s",
                log_prefix,
                request.unit_uuid,
                html_to_log_text(error_html),
            )
    else:
        context.log.warning("%s: нет данных для unit %s", log_prefix, request.unit_uuid)

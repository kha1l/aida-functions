"""Общие helpers для регулярных job, которые обходят несколько заведений и шлют отчеты в RabbitMQ."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Mapping, Optional, Tuple

import aiohttp
from dagster import OpExecutionContext

from database.models import Account, Token, Unit
from database.requests import (
    ReportSubscription,
    load_accounts_and_tokens_bulk,
    load_units_by_uuids,
    select_account_ids_for_units,
)
from functions.shared.manual_report_runtime import html_to_log_text, split_text_for_telegram
from functions.shared.i18n import normalize_language
from functions.shared.report_text_i18n import translate_report_text
from functions.shared.unit_time import resolve_unit_now
from messaging import build_payload


LoadSubscriptions = Callable[[object], Awaitable[List[ReportSubscription]]]
AccountAuthMap = Dict[int, Tuple[Optional[Account], Optional[Token]]]
LocalizedText = str | Mapping[str, str] | Callable[[str], str]


@dataclass(frozen=True, slots=True)
class ReportJobSnapshot:
    """Снимок подписок и токенов, которого хватает job на весь прогон."""

    subscriptions: List[ReportSubscription]
    subs_by_unit: Dict[str, List[ReportSubscription]]
    accounts_by_unit: Dict[str, List[int]]
    units: Dict[str, Unit]
    candidate_accounts_by_unit: Dict[str, List[int]]
    account_auth_map: AccountAuthMap

    def subscribers_for(self, unit_uuid: str) -> List[ReportSubscription]:
        """Возвращает подписчиков по точке."""
        return self.subs_by_unit.get(unit_uuid, [])

    def unit_name_for(self, unit_uuid: str) -> str:
        """Возвращает человекочитаемое имя точки."""
        unit = self.units.get(unit_uuid)
        return unit.name if unit and unit.name else unit_uuid

    def unit_for(self, unit_uuid: str) -> Optional[Unit]:
        """Возвращает модель точки, если она найдена в БД."""
        return self.units.get(unit_uuid)

    def candidate_accounts_for(self, unit_uuid: str) -> List[int]:
        """Возвращает список токенов-кандидатов с fallback на account_id подписок."""
        order_account_ids = list({acc for acc in self.accounts_by_unit.get(unit_uuid, [])})
        return self.candidate_accounts_by_unit.get(unit_uuid, []) or order_account_ids

    def account_for(self, account_id: int) -> Optional[Account]:
        """Возвращает кешированный аккаунт по account_id."""
        account, _ = self.account_auth_map.get(account_id, (None, None))
        return account


async def load_report_job_snapshot(
    db,
    *,
    load_subscriptions: LoadSubscriptions,
) -> ReportJobSnapshot:
    """Грузит subscriptions/units/accounts одним bulk-срезом для регулярного job."""
    subscriptions = await load_subscriptions(db)

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

    # Собираем единый набор account_id заранее, чтобы один раз прогреть кеш токенов
    # и потом не ходить в БД по каждому заведению отдельно.
    all_account_ids = _collect_snapshot_account_ids(
        accounts_by_unit,
        candidate_accounts_by_unit,
    )
    account_auth_map = await load_accounts_and_tokens_bulk(db, all_account_ids)

    return ReportJobSnapshot(
        subscriptions=subscriptions,
        subs_by_unit=subs_by_unit,
        accounts_by_unit=accounts_by_unit,
        units=units,
        candidate_accounts_by_unit=candidate_accounts_by_unit,
        account_auth_map=account_auth_map,
    )


@asynccontextmanager
async def report_job_runtime(
    dodo_api,
    rabbitmq,
    *,
    ai_client=None,
):
    """Поднимает общие runtime-зависимости job: aiohttp session, limiter и RabbitMQ."""
    request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))
    dodo_timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)

    async with AsyncExitStack() as stack:
        dodo_session = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=dodo_timeout, trust_env=True)
        )
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=request_semaphore,
        )

        if ai_client is not None:
            ai_timeout = aiohttp.ClientTimeout(total=ai_client.timeout_seconds)
            ai_session = await stack.enter_async_context(
                aiohttp.ClientSession(timeout=ai_timeout, trust_env=True)
            )
            ai_client.configure_runtime(session=ai_session)

        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        try:
            yield
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            if greeting_ai is not None:
                greeting_ai.reset_runtime()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def publish_report_to_subscribers(
    rabbitmq,
    *,
    publish_lock: asyncio.Lock,
    subscribers: List[ReportSubscription],
    post: str,
    text: LocalizedText,
    unit_uuid: str,
    prefer_paragraph_split: bool = False,
) -> None:
    """Режет длинный отчет и отправляет его всем подписчикам точки."""
    localized_text_cache: Dict[str, str] = {}

    for sub in subscribers:
        language = normalize_language(sub.language)
        report_text = _resolve_localized_text(
            text,
            post=post,
            language=language,
            cache=localized_text_cache,
        )
        for text_chunk in split_text_for_telegram(
            report_text,
            prefer_paragraph_split=prefer_paragraph_split,
        ):
            payload = build_payload(
                chat_id=sub.chat_id,
                topic_id=sub.topic_id,
                post=post,
                text=text_chunk,
                unit_uuid=unit_uuid,
            )
            async with publish_lock:
                await rabbitmq.publish_async(payload, platform=sub.platform)


def log_auth_error(
    context: OpExecutionContext,
    *,
    log_prefix: str,
    unit_uuid: str,
    account_id: int,
    error_html: Optional[str],
    error_status: Optional[int],
) -> None:
    """Пишет в лог только те ошибки токена, на которые стоит смотреть сразу."""
    if error_html and error_status in {401, 403}:
        context.log.warning(
            "%s: ошибка Dodo API для unit %s, account_id=%s: %s",
            log_prefix,
            unit_uuid,
            account_id,
            html_to_log_text(error_html),
        )


def log_final_unit_error(
    context: OpExecutionContext,
    *,
    log_prefix: str,
    unit_uuid: str,
    error_html: Optional[str],
    error_status: Optional[int],
    empty_message: Optional[str] = None,
) -> None:
    """Единый лог финальной ошибки после всех retry и fallback по токенам."""
    if error_html:
        if error_status in {429, 500}:
            context.log.error(
                "%s: не удалось собрать отчет для unit %s после retry: %s",
                log_prefix,
                unit_uuid,
                html_to_log_text(error_html),
            )
        elif error_status not in {401, 403}:
            context.log.error(
                "%s: не удалось собрать отчет для unit %s: %s",
                log_prefix,
                unit_uuid,
                html_to_log_text(error_html),
            )
    elif empty_message:
        context.log.warning("%s: %s для unit %s", log_prefix, empty_message, unit_uuid)


def resolve_target_local_hour(snapshot: ReportJobSnapshot, unit_uuid: str) -> int:
    """Возвращает локальный час точки по данным snapshot."""
    return resolve_unit_now(snapshot.unit_for(unit_uuid)).hour


def _collect_snapshot_account_ids(
    accounts_by_unit: Dict[str, List[int]],
    candidate_accounts_by_unit: Dict[str, List[int]],
) -> set[int]:
    """Собирает все account_id, которые могут понадобиться job во время прогона."""
    result = {
        account_id
        for account_ids in accounts_by_unit.values()
        for account_id in account_ids
    }
    result.update(
        account_id
        for account_ids in candidate_accounts_by_unit.values()
        for account_id in account_ids
    )
    return result


def _resolve_localized_text(
    source: LocalizedText,
    *,
    post: str,
    language: str,
    cache: Dict[str, str],
) -> str:
    """Возвращает текст отчета для конкретного языка с простым кэшем."""
    if isinstance(source, str):
        if language not in cache:
            cache[language] = translate_report_text(post, language, source)
        return cache[language]
    if isinstance(source, Mapping):
        return source.get(language) or source.get("ru") or next(iter(source.values()))

    if language not in cache:
        cache[language] = source(language)
    return cache[language]

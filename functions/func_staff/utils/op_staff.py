"""Ежедневный job по юбилеям и годовщинам работы сотрудников."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from dagster import OpExecutionContext

from database.requests import (
    ReportSubscription,
    load_accounts_and_tokens_bulk,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_report_subscriptions,
)
from functions.shared.manual_report_runtime import html_to_log_text, split_text_for_telegram
from functions.shared.greeting_utils import normalize_single_sentence_greeting_html
from functions.shared.unit_time import resolve_unit_today
from messaging import build_payload

MAX_CONCURRENT_UNITS = 15
STAFF_MEMBERS_TAKE = 990
REMINDER_TOMORROW = "‼ Завтра"
REMINDER_TWO_WEEKS = "❕ Через 2 недели"
EVENT_JUBILEE = "jubilee"
EVENT_TENURE = "tenure"


@dataclass(frozen=True, slots=True)
class StaffEvent:
    """Будущее событие по сотруднику: юбилей или годовщина работы."""

    event_type: str
    when_label: str
    full_name: str
    first_name: str
    position_name: str
    years: int


async def run_staff(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных staff-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api
    openrouter = context.resources.openrouter

    subscriptions = await select_report_subscriptions(db, "staff")
    if not subscriptions:
        context.log.info("staff_op: нет подписок")
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
                    context.log.warning("staff_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                # Для напоминаний по юбилеям и стажу ориентируемся на локальную дату точки.
                local_today = resolve_unit_today(unit)
                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_staff_report(
                        dodo_api=dodo_api,
                        openrouter=openrouter,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                        local_today=local_today,
                        context=context,
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    if last_error_html and last_error_status in {401, 403}:
                        context.log.warning(
                            "staff_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            html_to_log_text(last_error_html),
                        )

                if not successful_lookup:
                    if last_error_html:
                        if last_error_status in {429, 500}:
                            context.log.error(
                                "staff_op: не удалось собрать отчет для unit %s после retry: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                        elif last_error_status not in {401, 403}:
                            context.log.error(
                                "staff_op: не удалось собрать отчет для unit %s: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                    return

                if not report_text:
                    return

                for text_chunk in split_text_for_telegram(report_text):
                    for sub in subscribers:
                        payload = build_payload(
                            chat_id=sub.chat_id,
                            topic_id=sub.topic_id,
                            post="staff",
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


async def _build_staff_report(
    *,
    dodo_api,
    openrouter,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    local_today: date,
    context: OpExecutionContext,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    members_result = await dodo_api.get(
        account_id,
        "staff/members",
        params={
            "units": unit_uuid,
            "statuses": "Active",
            "take": STAFF_MEMBERS_TAKE,
        },
    )
    if not members_result.ok:
        return None, members_result.error_html, members_result.status

    events = _extract_staff_events(
        data=members_result.data if isinstance(members_result.data, dict) else {},
        local_today=local_today,
    )
    if not events:
        return "", None, 200

    return await _format_staff_message(
        unit_name=unit_name,
        events=events,
        openrouter=openrouter,
        context=context,
    ), None, 200


def _extract_staff_events(
    *,
    data: dict,
    local_today: date,
) -> List[StaffEvent]:
    """Ищет будущие юбилеи и годовщины работы по правилам старого staff.py."""
    events: List[StaffEvent] = []

    for member in data.get("members", []) or []:
        if not isinstance(member, dict):
            continue

        first_name = str(member.get("firstName") or "").strip()
        last_name = str(member.get("lastName") or "").strip()
        full_name = " ".join(part for part in [last_name, first_name] if part).strip()
        if not full_name:
            full_name = "Неизвестный сотрудник"

        position_name = str(member.get("positionName") or "").strip()

        birth_date = _parse_date(member.get("dateOfBirth"))
        if birth_date is not None:
            birthday_event = _build_jubilee_event(
                full_name=full_name,
                first_name=first_name,
                position_name=position_name,
                birth_date=birth_date,
                local_today=local_today,
            )
            if birthday_event is not None:
                events.append(birthday_event)

        hired_on = _parse_date(member.get("hiredOn"))
        if hired_on is not None:
            tenure_event = _build_tenure_event(
                full_name=full_name,
                first_name=first_name,
                position_name=position_name,
                hired_on=hired_on,
                local_today=local_today,
            )
            if tenure_event is not None:
                events.append(tenure_event)

    events.sort(key=lambda item: (item.when_label, item.event_type, item.full_name))
    return events


def _build_jubilee_event(
    *,
    full_name: str,
    first_name: str,
    position_name: str,
    birth_date: date,
    local_today: date,
) -> Optional[StaffEvent]:
    target_date = _replace_year_safe(birth_date, local_today.year)
    when_label = _match_reminder_label(local_today, target_date)
    if when_label is None:
        return None

    years = local_today.year - birth_date.year
    if years <= 0 or years % 5 != 0:
        return None

    return StaffEvent(
        event_type=EVENT_JUBILEE,
        when_label=when_label,
        full_name=full_name,
        first_name=first_name,
        position_name=position_name,
        years=years,
    )


def _build_tenure_event(
    *,
    full_name: str,
    first_name: str,
    position_name: str,
    hired_on: date,
    local_today: date,
) -> Optional[StaffEvent]:
    target_date = _replace_year_safe(hired_on, local_today.year)
    when_label = _match_reminder_label(local_today, target_date)
    if when_label is None:
        return None

    years = local_today.year - hired_on.year
    if years <= 0:
        return None

    return StaffEvent(
        event_type=EVENT_TENURE,
        when_label=when_label,
        full_name=full_name,
        first_name=first_name,
        position_name=position_name,
        years=years,
    )


def _match_reminder_label(local_today: date, target_date: date) -> Optional[str]:
    """Возвращает подпись напоминания: завтра или через 2 недели."""
    if local_today == target_date - timedelta(days=1):
        return REMINDER_TOMORROW
    if local_today == target_date - timedelta(days=14):
        return REMINDER_TWO_WEEKS
    return None


async def _format_staff_message(
    *,
    unit_name: str,
    events: List[StaffEvent],
    openrouter,
    context: OpExecutionContext,
) -> str:
    """Собирает итоговое сообщение по юбилеям и стажу для одной точки."""
    lines = [f"🎉 Юбилеи/годовщины в заведении <b>{html.escape(unit_name)}</b>", ""]
    for event in events:
        lines.append(f"{event.when_label} {_event_title(event)}")
        lines.append(f"<b>{html.escape(event.full_name)}</b> {_event_icon(event)}")

        details: List[str] = []
        if event.position_name:
            details.append(html.escape(event.position_name))
        details.append(_event_years_label(event))
        lines.append(", ".join(details))
        lines.append(
            await _generate_staff_greeting(
                openrouter=openrouter,
                event=event,
                context=context,
                unit_name=unit_name,
            )
        )
        lines.append("")
    lines.append("#сотрудники")
    return "\n".join(lines).strip()


async def _generate_staff_greeting(
    *,
    openrouter,
    event: StaffEvent,
    context: OpExecutionContext,
    unit_name: str,
) -> str:
    """Генерирует короткое поздравление через OpenRouter."""
    details = [
        f"Должность: {event.position_name or 'сотрудник'}.",
        "Нужно ровно 1 короткое предложение.",
        "Текст должен быть доброжелательным и без мата.",
        "Верни только текст поздравления без заголовка, подписи и пояснений.",
        "Без эмодзи и без переносов строк.",
    ]
    if unit_name:
        details.append(f"Заведение: {unit_name}.")

    if event.event_type == EVENT_JUBILEE:
        occasion = f"{_when_phrase(event.when_label)} у сотрудника юбилей — {event.years} лет"
        age = event.years
    else:
        occasion = f"{_when_phrase(event.when_label)} у сотрудника годовщина работы — {event.years} лет стажа"
        age = None

    result = await openrouter.generate_greeting(
        employee_name=event.first_name or event.full_name,
        occasion=occasion,
        age=age,
        details=details,
        tone="дружелюбный, живой и теплый",
    )
    if not result.ok or not result.text:
        if result.error_html:
            context.log.warning(
                "staff_op: OpenRouter не сгенерировал поздравление для %s: %s",
                event.full_name,
                html_to_log_text(result.error_html),
            )
        return _fallback_staff_greeting(event)

    greeting = _normalize_greeting_text(result.text)
    if greeting is None:
        context.log.warning(
            "staff_op: OpenRouter вернул неожиданный формат поздравления для %s",
            event.full_name,
        )
        return _fallback_staff_greeting(event)
    return greeting


def _normalize_greeting_text(value: str) -> Optional[str]:
    """Нормализует ответ модели и оборачивает его в курсив для Telegram HTML."""
    return normalize_single_sentence_greeting_html(value)


def _fallback_staff_greeting(event: StaffEvent) -> str:
    """Запасное короткое поздравление на случай недоступности AI."""
    first_name = event.first_name or event.full_name
    if event.event_type == EVENT_JUBILEE:
        return (
            "<i>"
            f"{html.escape(first_name)}, юбилей уже рядом — пусть он принесет много радости, теплых слов и новых поводов гордиться собой."
            "</i>"
        )
    return (
        "<i>"
        f"{html.escape(first_name)}, с годовщиной работы — пусть впереди будет еще больше уверенности, признания и хороших рабочих дней."
        "</i>"
    )


def _event_title(event: StaffEvent) -> str:
    return "юбилей" if event.event_type == EVENT_JUBILEE else "годовщина работы"


def _event_icon(event: StaffEvent) -> str:
    return "💐" if event.event_type == EVENT_JUBILEE else "💼"


def _event_years_label(event: StaffEvent) -> str:
    return f"Возраст: {event.years}" if event.event_type == EVENT_JUBILEE else f"Стаж: {event.years}"


def _when_phrase(when_label: str) -> str:
    if when_label == REMINDER_TOMORROW:
        return "завтра"
    return "через 2 недели"


def _parse_date(value: object) -> Optional[date]:
    """Преобразует строку YYYY-MM-DD в date."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _replace_year_safe(value: date, year: int) -> date:
    """Безопасно переносит дату в нужный год, учитывая 29 февраля."""
    try:
        return value.replace(year=year)
    except ValueError:
        return value.replace(year=year, day=28)

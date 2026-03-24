"""Ежедневный job по юбилеям и годовщинам работы сотрудников."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.requests import select_report_subscriptions
from functions.shared.i18n import pick
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.greeting_utils import normalize_single_sentence_greeting_html
from functions.shared.unit_time import resolve_unit_now, resolve_unit_today

MAX_CONCURRENT_UNITS = 15
STAFF_MEMBERS_TAKE = 990
TRIGGER_HOURS = set(schedule_local_hours("staff", (9,)))
REMINDER_TOMORROW = "tomorrow"
REMINDER_TWO_WEEKS = "two_weeks"
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

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "staff"),
    )
    if not snapshot.subscriptions:
        context.log.info("staff_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    unit_uuids = [
        unit_uuid
        for unit_uuid in snapshot.subs_by_unit.keys()
        if resolve_unit_now(snapshot.unit_for(unit_uuid)).hour in TRIGGER_HOURS
    ]
    if not unit_uuids:
        context.log.info(
            "staff_op: нет точек на локальные часы из SCHEDULE_STAFF_HOURS",
        )
        return

    async with report_job_runtime(dodo_api, rabbitmq, openrouter=openrouter):
        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                subscribers = snapshot.subscribers_for(unit_uuid)
                unit = snapshot.unit_for(unit_uuid)
                unit_name = snapshot.unit_name_for(unit_uuid)

                if not subscribers:
                    return

                candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
                if not candidate_accounts:
                    context.log.warning("staff_op: нет токенов для unit %s", unit_uuid)
                    return

                report_texts: dict[str, str] = {}
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False
                local_today = resolve_unit_today(unit)

                for account_id in candidate_accounts:
                    report_texts, last_error_html, last_error_status = await _build_staff_report(
                        dodo_api=dodo_api,
                        openrouter=openrouter,
                        unit_uuid=unit_uuid,
                        unit_name=unit_name,
                        account_id=account_id,
                        local_today=local_today,
                        context=context,
                        languages={sub.language for sub in subscribers},
                    )
                    if last_error_html is None and last_error_status == 200:
                        successful_lookup = True
                        break
                    log_auth_error(
                        context,
                        log_prefix="staff_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="staff_op",
                        unit_uuid=unit_uuid,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )
                    return

                if not report_texts:
                    return

                await publish_report_to_subscribers(
                    rabbitmq,
                    publish_lock=publish_lock,
                    subscribers=subscribers,
                    post="staff",
                    text=report_texts,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def _build_staff_report(
    *,
    dodo_api,
    openrouter,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    local_today: date,
    context: OpExecutionContext,
    languages: set[str],
) -> tuple[dict[str, str], Optional[str], Optional[int]]:
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
        return {}, members_result.error_html, members_result.status

    events = _extract_staff_events(
        data=members_result.data if isinstance(members_result.data, dict) else {},
        local_today=local_today,
    )
    if not events:
        return {}, None, 200

    report_texts = {
        language: await _format_staff_message(
            unit_name=unit_name,
            events=events,
            openrouter=openrouter,
            context=context,
            language=language,
        )
        for language in languages or {"ru"}
    }
    return report_texts, None, 200


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
            full_name = "Unknown employee"

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
    language: str,
) -> str:
    """Собирает итоговое сообщение по юбилеям и стажу для одной точки."""
    lines = [f"🎉 {pick(language, 'Юбилеи/годовщины в заведении', 'Anniversaries at')} <b>{html.escape(unit_name)}</b>", ""]
    for event in events:
        lines.append(f"{_when_title(event.when_label, language)} {_event_title(event, language)}")
        lines.append(f"<b>{html.escape(event.full_name)}</b> {_event_icon(event)}")

        details: List[str] = []
        if event.position_name:
            details.append(html.escape(event.position_name))
        details.append(_event_years_label(event, language))
        lines.append(", ".join(details))
        lines.append(
            await _generate_staff_greeting(
                openrouter=openrouter,
                event=event,
                context=context,
                unit_name=unit_name,
                language=language,
            )
        )
        lines.append("")
    lines.append(pick(language, "#сотрудники", "#staff"))
    return "\n".join(lines).strip()


async def _generate_staff_greeting(
    *,
    openrouter,
    event: StaffEvent,
    context: OpExecutionContext,
    unit_name: str,
    language: str,
) -> str:
    """Генерирует короткое поздравление через OpenRouter."""
    details = [
        pick(language, "Нужно ровно 1 короткое предложение.", "Need exactly 1 short sentence."),
        pick(language, "Текст должен быть доброжелательным и без мата.", "The text should be warm and without profanity."),
        pick(language, "Верни только текст поздравления без заголовка, подписи и пояснений.", "Return only the greeting text without a title, signature or explanations."),
        pick(language, "Без эмодзи и без переносов строк.", "No emojis and no line breaks."),
        pick(language, "Поздравллять надо так, как будто праздник сегодня.", "You should congratulate as if it were a holiday today."),
    ]
    if unit_name:
        details.append(f"{pick(language, 'Заведение', 'Location')}: {unit_name}.")

    if event.event_type == EVENT_JUBILEE:
        occasion = pick(
            language,
            f"{_when_phrase(event.when_label, language)} у сотрудника юбилей — {event.years} лет",
            f"{_when_phrase(event.when_label, language)} the employee has a jubilee — {event.years} years old",
        )
        age = event.years
    else:
        occasion = pick(
            language,
            f"{_when_phrase(event.when_label, language)} у сотрудника годовщина работы — {event.years} лет стажа",
            f"{_when_phrase(event.when_label, language)} the employee has a work anniversary — {event.years} years of tenure",
        )
        age = None

    result = await openrouter.generate_greeting(
        employee_name=event.first_name or event.full_name,
        occasion=occasion,
        language=language,
        age=age,
        details=details,
        tone=pick(language, "дружелюбный, живой и теплый", "friendly, lively and warm"),
    )
    if not result.ok or not result.text:
        if result.error_html:
            context.log.warning(
                "staff_op: OpenRouter не сгенерировал поздравление для %s: %s",
                event.full_name,
                html_to_log_text(result.error_html),
            )
        return _fallback_staff_greeting(event, language)

    greeting = _normalize_greeting_text(result.text)
    if greeting is None:
        context.log.warning(
            "staff_op: OpenRouter вернул неожиданный формат поздравления для %s",
            event.full_name,
        )
        return _fallback_staff_greeting(event, language)
    return greeting


def _normalize_greeting_text(value: str) -> Optional[str]:
    """Нормализует ответ модели и оборачивает его в курсив для Telegram HTML."""
    return normalize_single_sentence_greeting_html(value)


def _fallback_staff_greeting(event: StaffEvent, language: str) -> str:
    """Запасное короткое поздравление на случай недоступности AI."""
    first_name = event.first_name or event.full_name
    if event.event_type == EVENT_JUBILEE:
        return (
            "<i>"
            + f"{html.escape(first_name)}, "
            + pick(
                language,
                "юбилей уже рядом — пусть он принесет много радости, теплых слов и новых поводов гордиться собой.",
                "your jubilee is almost here — may it bring joy, warm words and many new reasons to be proud of yourself.",
            )
            + "</i>"
        )
    return (
        "<i>"
        + f"{html.escape(first_name)}, "
        + pick(
            language,
            "с годовщиной работы — пусть впереди будет еще больше уверенности, признания и хороших рабочих дней.",
            "happy work anniversary — may the days ahead bring even more confidence, recognition and good working days.",
        )
        + "</i>"
    )


def _event_title(event: StaffEvent, language: str) -> str:
    return pick(language, "юбилей", "jubilee") if event.event_type == EVENT_JUBILEE else pick(language, "годовщина работы", "work anniversary")


def _event_icon(event: StaffEvent) -> str:
    return "💐" if event.event_type == EVENT_JUBILEE else "💼"


def _event_years_label(event: StaffEvent, language: str) -> str:
    return (
        f"{pick(language, 'Возраст', 'Age')}: {event.years}"
        if event.event_type == EVENT_JUBILEE
        else f"{pick(language, 'Стаж', 'Tenure')}: {event.years}"
    )


def _when_phrase(when_label: str, language: str) -> str:
    if when_label == REMINDER_TOMORROW:
        return pick(language, "завтра", "tomorrow")
    return pick(language, "через 2 недели", "in two weeks")


def _when_title(when_label: str, language: str) -> str:
    if when_label == REMINDER_TOMORROW:
        return pick(language, "‼ Завтра", "‼ Tomorrow")
    return pick(language, "❕ Через 2 недели", "❕ In two weeks")


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

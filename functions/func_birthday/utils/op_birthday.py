"""Ежедневный job по дням рождения сотрудников и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional

from dagster import OpExecutionContext

from config import schedule_local_hours
from database.requests import select_report_subscriptions
from functions.shared.daily_job_config import (
    configured_report_date,
    is_configured_run,
    select_configured_unit_uuids,
)
from functions.shared.report_job_runtime import (
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.greeting_utils import normalize_single_sentence_greeting_html
from functions.shared.i18n import is_english, pick
from functions.shared.unit_time import resolve_report_local_date, resolve_unit_now

MAX_CONCURRENT_UNITS = 15
STAFF_MEMBERS_TAKE = 990
TRIGGER_HOURS = set(schedule_local_hours("birthday", (8,)))


@dataclass(frozen=True, slots=True)
class BirthdayEmployee:
    """Сотрудник, у которого сегодня день рождения по локальной дате точки."""

    full_name: str
    first_name: str
    position_name: str
    age: Optional[int]


async def run_birthday(context: OpExecutionContext) -> None:
    """Оркестрирует сбор и отправку ежедневных birthday-отчетов."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api
    ai_client = context.resources.zveno

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions(db_, "birthday"),
    )
    if not snapshot.subscriptions:
        context.log.info("birthday_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()
    configured_run = is_configured_run(context)
    report_date = configured_report_date(context)
    if configured_run:
        unit_uuids = select_configured_unit_uuids(
            context,
            available_unit_uuids=snapshot.subs_by_unit.keys(),
            log_prefix="birthday_op",
        )
    else:
        unit_uuids = [
            unit_uuid
            for unit_uuid in snapshot.subs_by_unit.keys()
            if resolve_unit_now(snapshot.unit_for(unit_uuid)).hour in TRIGGER_HOURS
        ]
    if not unit_uuids:
        if configured_run:
            context.log.info("birthday_op: нет точек для заданных параметров запуска")
        else:
            context.log.info(
                "birthday_op: нет точек на локальные часы из SCHEDULE_BIRTHDAY_HOURS",
            )
        return

    async with report_job_runtime(dodo_api, rabbitmq, ai_client=ai_client):
        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                subscribers = snapshot.subscribers_for(unit_uuid)
                unit = snapshot.unit_for(unit_uuid)
                unit_name = snapshot.unit_name_for(unit_uuid)

                if not subscribers:
                    return

                candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
                if not candidate_accounts:
                    context.log.warning("birthday_op: нет токенов для unit %s", unit_uuid)
                    return

                report_texts: dict[str, str] = {}
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False
                local_today = resolve_report_local_date(
                    unit,
                    report_date,
                )

                for account_id in candidate_accounts:
                    report_texts, last_error_html, last_error_status = await _build_birthday_report(
                        dodo_api=dodo_api,
                        ai_client=ai_client,
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
                        log_prefix="birthday_op",
                        unit_uuid=unit_uuid,
                        account_id=account_id,
                        error_html=last_error_html,
                        error_status=last_error_status,
                    )

                if not successful_lookup:
                    log_final_unit_error(
                        context,
                        log_prefix="birthday_op",
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
                    post="birthday",
                    text=report_texts,
                    unit_uuid=unit_uuid,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in unit_uuids))


async def _build_birthday_report(
    *,
    dodo_api,
    ai_client,
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

    employees = _extract_birthday_employees(
        data=members_result.data if isinstance(members_result.data, dict) else {},
        local_today=local_today,
    )
    if not employees:
        return {}, None, 200

    report_texts = {
        language: await _format_birthday_message(
            unit_name=unit_name,
            employees=employees,
            ai_client=ai_client,
            context=context,
            language=language,
        )
        for language in languages or {"ru"}
    }
    return report_texts, None, 200


def _extract_birthday_employees(
    *,
    data: dict,
    local_today: date,
) -> List[BirthdayEmployee]:
    """Фильтрует сотрудников, у которых день рождения совпадает с локальной датой точки."""
    employees: List[BirthdayEmployee] = []

    for member in data.get("members", []) or []:
        if not isinstance(member, dict):
            continue

        birth_date = _parse_birth_date(member.get("dateOfBirth"))
        if birth_date is None:
            continue
        if (birth_date.month, birth_date.day) != (local_today.month, local_today.day):
            continue

        first_name = str(member.get("firstName") or "").strip()
        last_name = str(member.get("lastName") or "").strip()
        full_name = " ".join(part for part in [last_name, first_name] if part).strip()
        if not full_name:
            full_name = "Unknown employee"

        position_name = str(member.get("positionName") or "").strip()
        age = local_today.year - birth_date.year

        employees.append(
            BirthdayEmployee(
                full_name=full_name,
                first_name=first_name,
                position_name=position_name,
                age=age,
            ))

    employees.sort(key=lambda item: item.full_name)
    return employees


async def _format_birthday_message(
    *,
    unit_name: str,
    employees: List[BirthdayEmployee],
    ai_client,
    context: OpExecutionContext,
    language: str,
) -> str:
    """Собирает итоговое сообщение по дням рождения для одной точки."""
    lines = [f"🎉 {pick(language, 'Дни рождения в заведении', 'Birthdays at')} <b>{html.escape(unit_name)}</b>", ""]
    for employee in employees:
        lines.append(f"<b>{html.escape(employee.full_name)}</b> 🎁")
        details: List[str] = []
        if employee.position_name:
            details.append(html.escape(employee.position_name))
        if employee.age is not None:
            details.append(f"{pick(language, 'Возраст', 'Age')}: {employee.age}")
        if details:
            lines.append(", ".join(details))
        lines.append(
            await _generate_birthday_greeting(
                ai_client=ai_client,
                employee=employee,
                context=context,
                unit_name=unit_name,
                language=language,
            )
        )
        lines.append("")
    lines.append(pick(language, "#дниРождения", "#birthdays"))
    return "\n".join(lines).strip()


async def _generate_birthday_greeting(
    *,
    ai_client,
    employee: BirthdayEmployee,
    context: OpExecutionContext,
    unit_name: str,
    language: str,
) -> str:
    """Генерирует короткое поздравление через Zveno и безопасно откатывается на шаблон."""
    details = [
        # {pick(language, 'Должность', 'Role')}: {employee.position_name or pick(language, 'сотрудник', 'employee')}.",
        f"{pick(language, "Нужно ровно 1 короткое предложение.", "Need exactly 1 short sentence.")}",
        pick(language, "Текст должен быть доброжелательным и без мата.", "The text should be warm and without profanity."),
        pick(language, "Верни только текст поздравления без заголовка, подписи и пояснений.", "Return only the greeting text without a title, signature or explanations."),
        pick(language, "Без эмодзи и без переносов строк.", "No emojis and no line breaks."),
    ]
    if unit_name:
        details.append(f"{pick(language, 'Заведение', 'Location')}: {unit_name}.")

    return await ai_client.generate_greeting_or_fallback(
        employee_name=employee.first_name or employee.full_name,
        occasion=pick(language, "день рождения", "birthday"),
        language=language,
        age=employee.age,
        details=details,
        tone=pick(language, "дружелюбный, живой и теплый", "friendly, lively and warm"),
        fallback=lambda: _fallback_birthday_greeting(employee, language),
        normalize=_normalize_greeting_text,
        log_warning=context.log.warning,
        log_prefix="birthday_op",
        subject=employee.full_name,
    )


def _normalize_greeting_text(value: str) -> Optional[str]:
    """Нормализует ответ модели и оборачивает его в курсив для Telegram HTML."""
    return normalize_single_sentence_greeting_html(value)


def _fallback_birthday_greeting(employee: BirthdayEmployee, language: str) -> str:
    """Возвращает запасное короткое поздравление, если AI временно недоступен."""
    first_name = employee.first_name or employee.full_name
    role = employee.position_name or pick(language, "команда", "team")
    return (
        "<i>"
        + (
            f"{html.escape(first_name)}, с днем рождения — пусть работа в роли {html.escape(role)} радует, а впереди будет много теплых и счастливых моментов."
            if not is_english(language) else
            f"{html.escape(first_name)}, happy birthday — may your work as {html.escape(role)} bring joy and may many warm and happy moments be ahead."
        )
        + "</i>"
    )


def _parse_birth_date(value: object) -> Optional[date]:
    """Преобразует строку YYYY-MM-DD в date."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

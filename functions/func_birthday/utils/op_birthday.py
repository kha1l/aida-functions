"""Ежедневный job по дням рождения сотрудников и отправка в RabbitMQ."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import date, datetime
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
    openrouter = context.resources.openrouter

    subscriptions = await select_report_subscriptions(db, "birthday")
    if not subscriptions:
        context.log.info("birthday_op: нет подписок")
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
                    context.log.warning("birthday_op: нет токенов для unit %s", unit_uuid)
                    return

                report_text: Optional[str] = None
                last_error_html: Optional[str] = None
                last_error_status: Optional[int] = None
                successful_lookup = False

                # Дату дня рождения считаем по локальной дате точки, а не по времени сервера.
                local_today = resolve_unit_today(unit)
                for account_id in candidate_accounts:
                    report_text, last_error_html, last_error_status = await _build_birthday_report(
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
                            "birthday_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            html_to_log_text(last_error_html),
                        )

                if not successful_lookup:
                    if last_error_html:
                        if last_error_status in {429, 500}:
                            context.log.error(
                                "birthday_op: не удалось собрать отчет для unit %s после retry: %s",
                                unit_uuid,
                                html_to_log_text(last_error_html),
                            )
                        elif last_error_status not in {401, 403}:
                            context.log.error(
                                "birthday_op: не удалось собрать отчет для unit %s: %s",
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
                            post="birthday",
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


async def _build_birthday_report(
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

    employees = _extract_birthday_employees(
        data=members_result.data if isinstance(members_result.data, dict) else {},
        local_today=local_today,
    )
    if not employees:
        return "", None, 200

    return await _format_birthday_message(
        unit_name=unit_name,
        employees=employees,
        openrouter=openrouter,
        context=context,
    ), None, 200


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
            full_name = "Неизвестный сотрудник"

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
    openrouter,
    context: OpExecutionContext,
) -> str:
    """Собирает итоговое сообщение по дням рождения для одной точки."""
    lines = [f"🎉 Дни рождения в заведении <b>{html.escape(unit_name)}</b>", ""]
    for employee in employees:
        lines.append(f"<b>{html.escape(employee.full_name)}</b> 🎁")
        details: List[str] = []
        if employee.position_name:
            details.append(html.escape(employee.position_name))
        if employee.age is not None:
            details.append(f"Возраст: {employee.age}")
        if details:
            lines.append(", ".join(details))
        lines.append(
            await _generate_birthday_greeting(
                openrouter=openrouter,
                employee=employee,
                context=context,
                unit_name=unit_name,
            )
        )
        lines.append("")
    lines.append("#дниРождения")
    return "\n".join(lines).strip()


async def _generate_birthday_greeting(
    *,
    openrouter,
    employee: BirthdayEmployee,
    context: OpExecutionContext,
    unit_name: str,
) -> str:
    """Генерирует короткое поздравление через OpenRouter и безопасно откатывается на шаблон."""
    details = [
        f"Должность: {employee.position_name or 'сотрудник'}.",
        "Нужно ровно 1 короткое предложение.",
        "Текст должен быть доброжелательным и без мата.",
        "Верни только текст поздравления без заголовка, подписи и пояснений.",
        "Без эмодзи и без переносов строк.",
    ]
    if unit_name:
        details.append(f"Заведение: {unit_name}.")

    result = await openrouter.generate_greeting(
        employee_name=employee.first_name or employee.full_name,
        occasion="день рождения",
        age=employee.age,
        details=details,
        tone="дружелюбный, живой и теплый",
    )
    if not result.ok or not result.text:
        if result.error_html:
            context.log.warning(
                "birthday_op: OpenRouter не сгенерировал поздравление для %s: %s",
                employee.full_name,
                html_to_log_text(result.error_html),
            )
        return _fallback_birthday_greeting(employee)

    greeting = _normalize_greeting_text(result.text)
    if greeting is None:
        context.log.warning(
            "birthday_op: OpenRouter вернул неожиданный формат поздравления для %s",
            employee.full_name,
        )
        return _fallback_birthday_greeting(employee)
    return greeting


def _normalize_greeting_text(value: str) -> Optional[str]:
    """Нормализует ответ модели и оборачивает его в курсив для Telegram HTML."""
    return normalize_single_sentence_greeting_html(value)


def _fallback_birthday_greeting(employee: BirthdayEmployee) -> str:
    """Возвращает запасное короткое поздравление, если AI временно недоступен."""
    first_name = employee.first_name or employee.full_name
    role = employee.position_name or "команда"
    return (
        "<i>"
        f"{html.escape(first_name)}, с днем рождения — пусть работа в роли {html.escape(role)} радует, а впереди будет много теплых и счастливых моментов."
        "</i>"
    )


def _parse_birth_date(value: object) -> Optional[date]:
    """Преобразует строку YYYY-MM-DD в date."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

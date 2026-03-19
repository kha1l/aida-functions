from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from messaging import build_payload

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_TELEGRAM_CHUNK = 4000
LATE_THRESHOLD_MINUTES = 10
PAGE_SIZE = 990


@dataclass(frozen=True, slots=True)
class StaffProfile:
    full_name: str
    sex: Optional[str]


@dataclass(frozen=True, slots=True)
class ScheduledShift:
    staff_id: str
    start_at: datetime
    end_at: datetime


@dataclass(frozen=True, slots=True)
class ActualShift:
    staff_id: str
    clock_in_at: datetime
    clock_out_at: Optional[datetime]


async def run_later(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по опозданиям/прогулам для одной точки."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    op_config = context.op_config
    unit_uuid = op_config["unit_uuid"]
    account_id = op_config["account_id"]
    chat_id = op_config["chat_id"]
    topic_id = op_config.get("topic_id")
    message_date = op_config.get("message_date")

    units = await load_units_by_uuids(db, [unit_uuid])
    unit = units.get(unit_uuid)
    unit_name = unit.name if unit and unit.name else unit_uuid

    account_auth_map = await load_accounts_and_tokens_bulk(db, {account_id})
    dodo_api.prime_account_token_cache(account_auth_map)

    request_semaphore = None
    if getattr(dodo_api, "max_concurrent_requests", None):
        request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))

    report_text: Optional[str] = None
    last_error_html: Optional[str] = None
    last_error_status: Optional[int] = None

    dodo_timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)
    async with aiohttp.ClientSession(timeout=dodo_timeout, trust_env=True) as dodo_session:
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=request_semaphore,
        )
        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        try:
            report_text, last_error_html, last_error_status = await _build_later_report(
                dodo_api=dodo_api,
                unit_uuid=unit_uuid,
                unit_name=unit_name,
                account_id=account_id,
                message_date=message_date,
            )

            if not report_text:
                if last_error_html:
                    if last_error_status in {401, 403}:
                        context.log.warning(
                            "later_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                            unit_uuid,
                            account_id,
                            _html_to_log_text(last_error_html),
                        )
                    elif last_error_status in {429, 500}:
                        context.log.error(
                            "later_op: не удалось собрать отчет для unit %s после retry: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                    else:
                        context.log.error(
                            "later_op: не удалось собрать отчет для unit %s: %s",
                            unit_uuid,
                            _html_to_log_text(last_error_html),
                        )
                else:
                    context.log.warning("later_op: нет данных для unit %s", unit_uuid)
                return

            for text_chunk in _split_text_for_telegram(report_text):
                payload = build_payload(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    post="later",
                    text=text_chunk,
                    unit_uuid=unit_uuid,
                )
                await rabbitmq.publish_async(payload)
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def _build_later_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    message_date: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = _build_report_window(message_date)

    schedule_params = {
        "units": unit_uuid,
        "staffType": "Courier",
        "beginFrom": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "beginTo": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "take": PAGE_SIZE,
    }
    shifts_params = {
        "units": unit_uuid,
        "staffTypeName": "Courier",
        "clockInFrom": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "clockInTo": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "take": PAGE_SIZE,
    }

    schedules_result, shifts_result = await asyncio.gather(
        dodo_api.get(account_id, "staff/schedules", params=schedule_params),
        dodo_api.get(account_id, "staff/shifts", params=shifts_params),
    )

    if not schedules_result.ok:
        return None, schedules_result.error_html, schedules_result.status
    if not shifts_result.ok:
        return None, shifts_result.error_html, shifts_result.status

    staff_cache = await _load_staff_cache(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
    )

    schedule_entries = _extract_schedules(schedules_result.data)
    shift_entries = _extract_shifts(shifts_result.data)
    report_text = await _format_later_message(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_name=unit_name,
        date_label=date_label,
        schedules=schedule_entries,
        shifts=shift_entries,
        staff_cache=staff_cache,
    )
    return report_text, None, 200


async def _load_staff_cache(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
) -> Dict[str, StaffProfile]:
    members_result = await dodo_api.get(
        account_id,
        "staff/members",
        params={
            "units": unit_uuid,
            "take": PAGE_SIZE,
        },
    )
    if not members_result.ok or not isinstance(members_result.data, dict):
        return {}

    staff_cache: Dict[str, StaffProfile] = {}
    for member in members_result.data.get("members", []) or []:
        if not isinstance(member, dict):
            continue
        staff_id = member.get("id") or member.get("staffId")
        if not isinstance(staff_id, str) or not staff_id:
            continue
        staff_cache[staff_id] = StaffProfile(
            full_name=_format_staff_name(member, fallback=staff_id),
            sex=_normalize_sex(member.get("sex")),
        )
    return staff_cache


def _extract_schedules(data: object) -> Dict[str, List[ScheduledShift]]:
    schedules_by_staff: Dict[str, List[ScheduledShift]] = {}
    if not isinstance(data, dict):
        return schedules_by_staff

    for raw_schedule in data.get("schedules", []) or []:
        if not isinstance(raw_schedule, dict):
            continue
        staff_id = raw_schedule.get("staffId")
        if not isinstance(staff_id, str) or not staff_id:
            continue
        start_at = _parse_dodo_datetime(raw_schedule.get("scheduledShiftStartAtLocal"))
        end_at = _parse_dodo_datetime(raw_schedule.get("scheduledShiftEndAtLocal"))
        if start_at is None or end_at is None or end_at <= start_at:
            continue
        schedules_by_staff.setdefault(staff_id, []).append(
            ScheduledShift(
                staff_id=staff_id,
                start_at=start_at,
                end_at=end_at,
            )
        )

    for entries in schedules_by_staff.values():
        entries.sort(key=lambda item: item.start_at)
    return schedules_by_staff


def _extract_shifts(data: object) -> List[ActualShift]:
    shifts: List[ActualShift] = []
    if not isinstance(data, dict):
        return shifts

    for raw_shift in data.get("shifts", []) or []:
        if not isinstance(raw_shift, dict):
            continue
        staff_id = raw_shift.get("staffId")
        if not isinstance(staff_id, str) or not staff_id:
            continue
        clock_in_at = _parse_dodo_datetime(raw_shift.get("clockInAtLocal"))
        if clock_in_at is None:
            continue
        shifts.append(
            ActualShift(
                staff_id=staff_id,
                clock_in_at=clock_in_at,
                clock_out_at=_parse_dodo_datetime(raw_shift.get("clockOutAtLocal")),
            )
        )

    shifts.sort(key=lambda item: item.clock_in_at)
    return shifts


async def _format_later_message(
    *,
    dodo_api,
    account_id: int,
    unit_name: str,
    date_label: str,
    schedules: Dict[str, List[ScheduledShift]],
    shifts: Iterable[ActualShift],
    staff_cache: Dict[str, StaffProfile],
) -> str:
    lines = [f"\U0000203C Опоздания/невыходы за {date_label} в заведении <b>{unit_name}</b>", ""]
    late_lines: List[str] = []
    early_leave_lines: List[str] = []
    wrong_shift_lines: List[str] = []
    absent_lines: List[str] = []

    remaining_schedules = {
        staff_id: list(entries)
        for staff_id, entries in schedules.items()
    }

    for shift in shifts:
        matched_schedule = _pop_best_schedule(
            remaining_schedules.get(shift.staff_id, []),
            shift,
        )
        staff = await _get_staff_profile(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=shift.staff_id,
        )

        if matched_schedule is None:
            verb = _verb_for_sex(staff.sex, male="вышел", female="вышла", neutral="вышел(а)")
            wrong_shift_lines.append(
                f"{staff.full_name} {verb} на работу не в свою смену"
            )
            continue

        late_minutes = int((shift.clock_in_at - matched_schedule.start_at).total_seconds() // 60)
        if late_minutes > LATE_THRESHOLD_MINUTES:
            verb = _verb_for_sex(staff.sex, male="опоздал", female="опоздала", neutral="опоздал(а)")
            late_lines.append(
                f"{staff.full_name} {verb} на {_format_minutes(late_minutes)}"
            )

        if shift.clock_out_at is not None:
            early_leave_minutes = int(
                (matched_schedule.end_at - shift.clock_out_at).total_seconds() // 60
            )
            if early_leave_minutes > LATE_THRESHOLD_MINUTES:
                verb = _verb_for_sex(staff.sex, male="ушел", female="ушла", neutral="ушел(ушла)")
                early_leave_lines.append(
                    f"{staff.full_name} {verb} раньше на {_format_minutes(early_leave_minutes)}"
                )

    for staff_id, unmatched_schedules in remaining_schedules.items():
        if not unmatched_schedules:
            continue
        staff = await _get_staff_profile(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=staff_id,
        )
        verb = _verb_for_sex(staff.sex, male="вышел", female="вышла", neutral="вышел(а)")
        absent_lines.append(f"{staff.full_name} не {verb} на работу")

    sections = [
        ("\U000023F0 Опоздания", late_lines),
        ("\U000023F3 Ушли раньше", early_leave_lines),
        ("\U00002753 Не своя смена", wrong_shift_lines),
        ("\U0001F6A8 Не вышли", absent_lines),
    ]

    if not any(items for _, items in sections):
        lines.append("Нарушений не найдено.")
    else:
        for title, items in sections:
            if not items:
                continue
            lines.append(title)
            lines.extend(items)
            lines.append("")

    return "\n".join(lines).rstrip()


async def _get_staff_profile(
    *,
    dodo_api,
    account_id: int,
    staff_cache: Dict[str, StaffProfile],
    staff_id: str,
) -> StaffProfile:
    cached = staff_cache.get(staff_id)
    if cached is not None:
        return cached

    member_result = await dodo_api.get(account_id, f"staff/members/{staff_id}")
    if not member_result.ok or not isinstance(member_result.data, dict):
        profile = StaffProfile(full_name=f"Сотрудник {staff_id}", sex=None)
        staff_cache[staff_id] = profile
        return profile

    profile = StaffProfile(
        full_name=_format_staff_name(member_result.data, fallback=staff_id),
        sex=_normalize_sex(member_result.data.get("sex")),
    )
    staff_cache[staff_id] = profile
    return profile


def _pop_best_schedule(
    schedules: List[ScheduledShift],
    shift: ActualShift,
) -> Optional[ScheduledShift]:
    if not schedules:
        return None

    best_index = min(
        range(len(schedules)),
        key=lambda idx: _schedule_distance(schedules[idx], shift),
    )
    return schedules.pop(best_index)


def _schedule_distance(schedule: ScheduledShift, shift: ActualShift) -> Tuple[float, float]:
    shift_end = shift.clock_out_at or shift.clock_in_at
    start_gap = abs((schedule.start_at - shift.clock_in_at).total_seconds())
    end_gap = abs((schedule.end_at - shift_end).total_seconds())
    return start_gap, end_gap


def _build_report_window(message_date: Optional[str]) -> Tuple[datetime, datetime, str]:
    reference_dt = _parse_reference_datetime(message_date) or datetime.now()
    dt_end = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    dt_start = dt_end - timedelta(days=1)
    return dt_start, dt_end, dt_start.strftime("%d.%m")


def _split_text_for_telegram(text: str) -> List[str]:
    if len(text) <= TELEGRAM_MESSAGE_LIMIT:
        return [text]

    chunks: List[str] = []
    current = text
    while len(current) > SAFE_TELEGRAM_CHUNK:
        split_at = current.rfind("\n", 0, SAFE_TELEGRAM_CHUNK)
        if split_at <= 0:
            split_at = SAFE_TELEGRAM_CHUNK
        chunks.append(current[:split_at].rstrip())
        current = current[split_at:].lstrip()
    if current:
        chunks.append(current)
    return chunks


def _html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _format_staff_name(member: Dict[str, object], *, fallback: str) -> str:
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = " ".join(part for part in [last_name, first_name] if part).strip()
    return full_name or f"Сотрудник {fallback}"


def _normalize_sex(value: object) -> Optional[str]:
    if value in {"Male", "Female"}:
        return str(value)
    return None


def _verb_for_sex(sex: Optional[str], *, male: str, female: str, neutral: str) -> str:
    if sex == "Male":
        return male
    if sex == "Female":
        return female
    return neutral


def _format_minutes(minutes: int) -> str:
    if minutes >= 60:
        hours = minutes // 60
        rest_minutes = minutes % 60
        return f"{hours}ч. {rest_minutes}м."
    return f"{minutes} м."


def _parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value.split(".", 1)[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed


def _parse_reference_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed

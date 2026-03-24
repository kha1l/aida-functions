"""Сбор manual-отчета по опозданиям и прогулам курьеров."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from dagster import OpExecutionContext

from functions.shared.i18n import pick

from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    format_staff_name,
    parse_dodo_datetime,
    run_manual_courier_report,
)

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
    await run_manual_courier_report(
        context,
        post="later",
        log_prefix="later_op",
        build_report=_build_later_report,
    )


async def _build_later_report(
    *,
    dodo_api,
    request: ManualCourierReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)

    schedule_params = {
        "units": request.unit_uuid,
        "staffType": "Courier",
        "beginFrom": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "beginTo": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "take": PAGE_SIZE,
    }
    shifts_params = {
        "units": request.unit_uuid,
        "staffTypeName": "Courier",
        "clockInFrom": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "clockInTo": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
        "take": PAGE_SIZE,
    }

    schedules_result, shifts_result = await asyncio.gather(
        dodo_api.get(request.account_id, "staff/schedules", params=schedule_params),
        dodo_api.get(request.account_id, "staff/shifts", params=shifts_params),
    )

    if not schedules_result.ok:
        return None, schedules_result.error_html, schedules_result.status
    if not shifts_result.ok:
        return None, shifts_result.error_html, shifts_result.status

    staff_cache = await _load_staff_cache(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_uuid=request.unit_uuid,
    )

    schedule_entries = _extract_schedules(schedules_result.data)
    shift_entries = _extract_shifts(shifts_result.data)
    report_text = await _format_later_message(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_name=request.unit_name,
        date_label=date_label,
        language=request.language,
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
            full_name=format_staff_name(member, fallback=staff_id),
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
        start_at = parse_dodo_datetime(raw_schedule.get("scheduledShiftStartAtLocal"))
        end_at = parse_dodo_datetime(raw_schedule.get("scheduledShiftEndAtLocal"))
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
        clock_in_at = parse_dodo_datetime(raw_shift.get("clockInAtLocal"))
        if clock_in_at is None:
            continue
        shifts.append(
            ActualShift(
                staff_id=staff_id,
                clock_in_at=clock_in_at,
                clock_out_at=parse_dodo_datetime(raw_shift.get("clockOutAtLocal")),
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
    language: str,
    schedules: Dict[str, List[ScheduledShift]],
    shifts: Iterable[ActualShift],
    staff_cache: Dict[str, StaffProfile],
) -> str:
    lines = [f"\U0000203C {pick(language, 'Опоздания/невыходы за', 'Late arrivals / absences for')} {date_label} {pick(language, 'в заведении', 'at')} <b>{unit_name}</b>", ""]
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
            wrong_shift_lines.append(_format_wrong_shift_line(staff.full_name, language))
            continue

        late_minutes = int((shift.clock_in_at - matched_schedule.start_at).total_seconds() // 60)
        if late_minutes > LATE_THRESHOLD_MINUTES:
            late_lines.append(_format_late_line(staff, late_minutes, language))

        if shift.clock_out_at is not None:
            early_leave_minutes = int(
                (matched_schedule.end_at - shift.clock_out_at).total_seconds() // 60
            )
            if early_leave_minutes > LATE_THRESHOLD_MINUTES:
                early_leave_lines.append(_format_early_leave_line(staff, early_leave_minutes, language))

    for staff_id, unmatched_schedules in remaining_schedules.items():
        if not unmatched_schedules:
            continue
        staff = await _get_staff_profile(
            dodo_api=dodo_api,
            account_id=account_id,
            staff_cache=staff_cache,
            staff_id=staff_id,
        )
        absent_lines.append(_format_absent_line(staff.full_name, language))

    sections = [
        (pick(language, "\U000023F0 Опоздания", "\U000023F0 Late arrivals"), late_lines),
        (pick(language, "\U000023F3 Ушли раньше", "\U000023F3 Left early"), early_leave_lines),
        (pick(language, "\U00002753 Не своя смена", "\U00002753 Wrong shift"), wrong_shift_lines),
        (pick(language, "\U0001F6A8 Не вышли", "\U0001F6A8 Absences"), absent_lines),
    ]

    if not any(items for _, items in sections):
        lines.append(pick(language, "Нарушений не найдено.", "No violations found."))
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
        profile = StaffProfile(full_name=f"Employee {staff_id}", sex=None)
        staff_cache[staff_id] = profile
        return profile

    profile = StaffProfile(
        full_name=format_staff_name(member_result.data, fallback=staff_id),
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


def _format_late_line(staff: StaffProfile, late_minutes: int, language: str) -> str:
    if language == "en":
        return f"{staff.full_name} was late by {_format_minutes(late_minutes, language)}"
    verb = _verb_for_sex(staff.sex, male="опоздал", female="опоздала", neutral="опоздал(а)")
    return f"{staff.full_name} {verb} на {_format_minutes(late_minutes, language)}"


def _format_early_leave_line(staff: StaffProfile, early_leave_minutes: int, language: str) -> str:
    if language == "en":
        return f"{staff.full_name} left early by {_format_minutes(early_leave_minutes, language)}"
    verb = _verb_for_sex(staff.sex, male="ушел", female="ушла", neutral="ушел(ушла)")
    return f"{staff.full_name} {verb} раньше на {_format_minutes(early_leave_minutes, language)}"


def _format_wrong_shift_line(full_name: str, language: str) -> str:
    return (
        f"{full_name} came to work outside the scheduled shift"
        if language == "en"
        else f"{full_name} вышел на работу не в свою смену"
    )


def _format_absent_line(full_name: str, language: str) -> str:
    return (
        f"{full_name} did not show up for the shift"
        if language == "en"
        else f"{full_name} не вышел на работу"
    )


def _format_minutes(minutes: int, language: str) -> str:
    if language == "en":
        if minutes >= 60:
            hours = minutes // 60
            rest_minutes = minutes % 60
            return f"{hours}h {rest_minutes}m"
        return f"{minutes}m"
    if minutes >= 60:
        hours = minutes // 60
        rest_minutes = minutes % 60
        return f"{hours}ч. {rest_minutes}м."
    return f"{minutes} м."

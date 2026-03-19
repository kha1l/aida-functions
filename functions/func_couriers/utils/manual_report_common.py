"""Общие helper'ы manual-отчетов по delivery-направлению."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from functions.shared.manual_report_runtime import (
    ManualUnitReportBuilder,
    ManualUnitReportRequest,
    build_report_window,
    format_clock,
    html_to_log_text,
    parse_reference_datetime,
    run_manual_unit_report,
    split_text_for_telegram as _shared_split_text_for_telegram,
    to_int,
)


ManualCourierReportRequest = ManualUnitReportRequest
ManualCourierReportBuilder = ManualUnitReportBuilder


async def run_manual_courier_report(
    context,
    *,
    post: str,
    log_prefix: str,
    build_report: ManualCourierReportBuilder,
) -> None:
    await run_manual_unit_report(
        context,
        post=post,
        log_prefix=log_prefix,
        build_report=build_report,
        prefer_paragraph_split=False,
    )


def split_text_for_telegram(text: str) -> list[str]:
    return _shared_split_text_for_telegram(text, prefer_paragraph_split=False)


def parse_dodo_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value or value.startswith("0001-01-01"):
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


def parse_unit_time(
    *,
    utc_value: object,
    local_value: object,
    unit_timezone: Optional[int],
) -> Optional[datetime]:
    parsed_utc = _parse_utc_datetime(utc_value)
    if parsed_utc is not None and unit_timezone is not None:
        shifted = parsed_utc.replace(tzinfo=timezone.utc).astimezone(
            timezone(timedelta(hours=unit_timezone))
        )
        return shifted.replace(tzinfo=None)

    parsed_local = parse_dodo_datetime(local_value)
    if parsed_local is not None:
        return parsed_local

    return parsed_utc


def format_staff_name(member: dict[str, object], *, fallback: str) -> str:
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = " ".join(part for part in [last_name, first_name] if part).strip()
    return full_name or f"Сотрудник {fallback}"


async def load_staff_name_cache(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    take: int = 990,
) -> dict[str, str]:
    members_result = await dodo_api.get(
        account_id,
        "staff/members",
        params={
            "units": unit_uuid,
            "take": take,
        },
    )
    if not members_result.ok or not isinstance(members_result.data, dict):
        return {}

    staff_cache: dict[str, str] = {}
    for member in members_result.data.get("members", []) or []:
        if not isinstance(member, dict):
            continue
        staff_id = member.get("id") or member.get("staffId")
        if not isinstance(staff_id, str) or not staff_id:
            continue
        staff_cache[staff_id] = format_staff_name(member, fallback=staff_id)
    return staff_cache


async def get_staff_name(
    *,
    dodo_api,
    account_id: int,
    staff_cache: dict[str, str],
    staff_id: object,
) -> str:
    if not isinstance(staff_id, str) or not staff_id:
        return "Неизвестный курьер"

    cached = staff_cache.get(staff_id)
    if cached is not None:
        return cached

    member_result = await dodo_api.get(account_id, f"staff/members/{staff_id}")
    if not member_result.ok or not isinstance(member_result.data, dict):
        staff_cache[staff_id] = "Неизвестный курьер"
        return staff_cache[staff_id]

    staff_cache[staff_id] = format_staff_name(member_result.data, fallback=staff_id)
    return staff_cache[staff_id]


def _parse_utc_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value or value.startswith("0001-01-01"):
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
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed

"""Config helpers for launching daily report jobs from Dagster web."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import List, Optional, Set

from dagster import Field


DAILY_REPORT_CONFIG_SCHEMA = {
    "unit_uuids": Field(
        [str],
        is_required=False,
        description="Optional list of unit UUIDs. When provided, the job ignores local-hour filtering.",
    ),
    "report_date": Field(
        str,
        is_required=False,
        description="Optional report date in YYYY-MM-DD format. Daily reports use this exact local date.",
    ),
}


def get_op_config(context) -> Mapping[str, object]:
    """Returns op config as a plain mapping."""
    op_config = getattr(context, "op_config", {}) or {}
    if isinstance(op_config, Mapping):
        return op_config
    return {}


def is_configured_run(context) -> bool:
    """True when the job was launched with Dagster web/manual parameters."""
    op_config = get_op_config(context)
    return "unit_uuids" in op_config or "report_date" in op_config


def configured_report_date(context) -> Optional[object]:
    """Returns raw report_date from op config."""
    return get_op_config(context).get("report_date")


def select_configured_unit_uuids(
    context,
    *,
    available_unit_uuids: Iterable[str],
    log_prefix: str,
) -> List[str]:
    """Filters configured unit UUIDs against subscribed units, preserving input order."""
    available = list(available_unit_uuids)
    requested = _normalize_unit_uuids(get_op_config(context).get("unit_uuids"))
    if not requested:
        return available

    available_set = set(available)
    selected = [unit_uuid for unit_uuid in requested if unit_uuid in available_set]
    missing = [unit_uuid for unit_uuid in requested if unit_uuid not in available_set]
    if missing:
        context.log.warning(
            "%s: запрошенные unit_uuids не найдены среди подписок: %s",
            log_prefix,
            ", ".join(missing),
        )
    return selected


def _normalize_unit_uuids(value: object) -> List[str]:
    if not isinstance(value, list):
        return []

    result: List[str] = []
    seen: Set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result

"""Сбор manual-отчета по ожиданиям курьеров."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
from dagster import OpExecutionContext

from database.requests import load_accounts_and_tokens_bulk, load_units_by_uuids
from functions.shared.manual_report_runtime import html_to_log_text
from functions.shared.webapp_results import store_webapp_report_result
from .manual_report_common import (
    ManualCourierReportRequest,
    build_report_window,
    format_clock,
    get_staff_name,
    load_staff_name_cache,
    parse_unit_time,
    run_manual_courier_report,
    to_int,
)

PAGE_SIZE = 500


@dataclass(frozen=True, slots=True)
class WaitingEntry:
    courier_name: str
    order_number: str
    ready_at: Optional[datetime]
    assembly_seconds: int
    heated_shelf_seconds: int
    queue_courier: int
    trip_start: Optional[datetime]
    trip_orders_count: int


async def run_waiting(context: OpExecutionContext) -> None:
    """Собирает и отправляет отчет по ожиданиям для одной точки."""
    if context.op_config.get("result_request_id"):
        await _run_waiting_webapp(context)
        return

    await run_manual_courier_report(
        context,
        post="waiting",
        log_prefix="waiting_op",
        build_report=_build_waiting_report,
    )


async def _build_waiting_report(
    *,
    dodo_api,
    request: ManualCourierReportRequest,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    dt_start, dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)
    params_base = {
        "units": request.unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    wait_threshold_seconds = int(request.op_config["wait_threshold_seconds"])

    staff_cache = await load_staff_name_cache(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_uuid=request.unit_uuid,
    )

    entries, error_html, error_status = await _fetch_waiting_entries(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
        staff_cache=staff_cache,
        unit_timezone=request.unit_timezone,
        wait_threshold_seconds=wait_threshold_seconds,
    )
    if entries is None:
        return None, error_html, error_status

    return _format_waiting_message(
        unit_name=request.unit_name,
        date_label=date_label,
        wait_threshold_seconds=wait_threshold_seconds,
        entries=entries,
    ), None, 200


async def _run_waiting_webapp(context: OpExecutionContext) -> None:
    """Собирает ожидания и сохраняет JSON-результат для WebApp без RabbitMQ."""
    db = context.resources.db
    dodo_api = context.resources.dodo_api
    op_config = context.op_config

    unit_uuid = op_config["unit_uuid"]
    account_id = int(op_config["account_id"])
    request_id = str(op_config.get("result_request_id") or "")

    units = await load_units_by_uuids(db, [unit_uuid])
    unit = units.get(unit_uuid)
    unit_name = unit.name if unit and unit.name else unit_uuid
    unit_timezone = unit.timezone if unit else None

    account_auth_map = await load_accounts_and_tokens_bulk(db, {account_id})
    dodo_api.prime_account_token_cache(account_auth_map)
    account, _ = account_auth_map.get(account_id, (None, None))

    request = ManualCourierReportRequest(
        unit_uuid=unit_uuid,
        unit_name=unit_name,
        unit_timezone=unit_timezone,
        account_id=account_id,
        account_properties=account.properties if account else None,
        chat_id=int(op_config.get("chat_id") or 0),
        topic_id=op_config.get("topic_id"),
        language="ru",
        platform="webapp",
        message_date=op_config.get("message_date"),
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
        try:
            try:
                payload = await _build_waiting_json_payload(dodo_api=dodo_api, request=request)
                payload["request_id"] = request_id
                await store_webapp_report_result(
                    db=db,
                    request_id=request_id,
                    payload=payload,
                    status="done" if payload.get("ok") else "error",
                    error=None if payload.get("ok") else str(payload.get("error") or "Report failed"),
                )
            except Exception as exc:
                await store_webapp_report_result(
                    db=db,
                    request_id=request_id,
                    payload={
                        "ok": False,
                        "post": "waiting",
                        "request_id": request_id,
                        "unit_uuid": unit_uuid,
                        "error": str(exc),
                    },
                    status="error",
                    error=str(exc),
                )
                raise
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()


async def _build_waiting_json_payload(
    *,
    dodo_api,
    request: ManualCourierReportRequest,
) -> dict[str, Any]:
    dt_start, dt_end, date_label = build_report_window(request.message_date, request.unit_timezone)
    wait_threshold_seconds = int(request.op_config["wait_threshold_seconds"])
    params_base = {
        "units": request.unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    staff_cache = await load_staff_name_cache(
        dodo_api=dodo_api,
        account_id=request.account_id,
        unit_uuid=request.unit_uuid,
    )
    entries, error_html, error_status = await _fetch_waiting_entries(
        dodo_api=dodo_api,
        account_id=request.account_id,
        params_base=params_base,
        staff_cache=staff_cache,
        unit_timezone=request.unit_timezone,
        wait_threshold_seconds=wait_threshold_seconds,
    )

    base_payload: dict[str, Any] = {
        "ok": entries is not None,
        "post": "waiting",
        "unit_uuid": request.unit_uuid,
        "unit_name": request.unit_name,
        "date_label": date_label,
        "window": {
            "from": dt_start.isoformat(),
            "to": dt_end.isoformat(),
        },
        "threshold_seconds": wait_threshold_seconds,
        "threshold_label": _format_threshold(wait_threshold_seconds),
    }

    if entries is None:
        base_payload.update(
            {
                "error": html_to_log_text(error_html or "Не удалось собрать отчет"),
                "error_status": error_status,
                "entries_count": 0,
                "entries": [],
            }
        )
        return base_payload

    base_payload.update(
        {
            "entries_count": len(entries),
            "entries": [_waiting_entry_to_json(entry) for entry in entries],
        }
    )
    return base_payload


def _waiting_entry_to_json(entry: WaitingEntry) -> dict[str, Any]:
    return {
        "courier_name": entry.courier_name,
        "order_number": entry.order_number,
        "ready_at": entry.ready_at.isoformat() if entry.ready_at else None,
        "ready_at_label": format_clock(entry.ready_at),
        "assembly_seconds": entry.assembly_seconds,
        "assembly_label": str(timedelta(seconds=entry.assembly_seconds)),
        "heated_shelf_seconds": entry.heated_shelf_seconds,
        "heated_shelf_label": str(timedelta(seconds=entry.heated_shelf_seconds)),
        "queue_courier": entry.queue_courier,
        "trip_start": entry.trip_start.isoformat() if entry.trip_start else None,
        "trip_start_label": format_clock(entry.trip_start),
        "trip_orders_count": entry.trip_orders_count,
    }


async def _fetch_waiting_entries(
    *,
    dodo_api,
    account_id: int,
    params_base: Dict[str, str],
    staff_cache: Dict[str, str],
    unit_timezone: Optional[int],
    wait_threshold_seconds: int,
) -> tuple[Optional[List[WaitingEntry]], Optional[str], Optional[int]]:
    skip = 0
    entries: List[WaitingEntry] = []

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": PAGE_SIZE})
        orders = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params=params,
        )
        if not orders.ok:
            return None, orders.error_html, orders.status

        data = orders.data if isinstance(orders.data, dict) else {}
        for order in data.get("couriersOrders", []) or []:
            if not isinstance(order, dict):
                continue
            wait = to_int(order.get("orderAssemblyAvgTime"))
            if wait is None or wait <= wait_threshold_seconds:
                continue

            courier_name = await get_staff_name(
                dodo_api=dodo_api,
                account_id=account_id,
                staff_cache=staff_cache,
                staff_id=order.get("courierStaffId"),
            )
            trip_start = parse_unit_time(
                utc_value=order.get("handedOverToDeliveryAt"),
                local_value=order.get("handedOverToDeliveryAtLocal"),
                unit_timezone=unit_timezone,
            )
            heated_shelf_seconds = to_int(order.get("heatedShelfTime")) or 0
            ready_at = (
                trip_start - timedelta(seconds=heated_shelf_seconds)
                if trip_start is not None
                else None
            )

            entries.append(
                WaitingEntry(
                    courier_name=courier_name,
                    order_number=str(order.get("orderNumber") or "-"),
                    ready_at=ready_at,
                    assembly_seconds=wait,
                    heated_shelf_seconds=heated_shelf_seconds,
                    queue_courier=to_int(order.get("numberOfCouriersInQueue")) or 0,
                    trip_start=trip_start,
                    trip_orders_count=to_int(order.get("tripOrdersCount")) or 0,
                )
            )

        if data.get("isEndOfListReached"):
            break
        skip += PAGE_SIZE

    entries.sort(key=lambda item: item.assembly_seconds, reverse=True)
    return entries, None, 200


def _format_waiting_message(
    *,
    unit_name: str,
    date_label: str,
    wait_threshold_seconds: int,
    entries: List[WaitingEntry],
) -> str:
    threshold_label = _format_threshold(wait_threshold_seconds)
    lines = [
        f"\U0001F4A4 <b>Ожидания за {date_label} в заведении {unit_name}</b>",
        f"Порог ожидания: больше {threshold_label}",
        "",
    ]

    if not entries:
        lines.append(
            f"Ожиданий дольше {threshold_label} не найдено. Отличная работа, так держать!"
        )
        return "\n".join(lines).rstrip()

    for entry in entries:
        lines.extend(
            [
                f"\U000026F7 Курьер: {entry.courier_name}",
                f"Номер заказа: {entry.order_number}",
                f"Время готовности заказа: {format_clock(entry.ready_at)}",
                f"Время сборки заказа: {timedelta(seconds=entry.assembly_seconds)}",
                f"Время заказа на полке: {timedelta(seconds=entry.heated_shelf_seconds)}",
                f"Количество курьеров в очереди: {entry.queue_courier}",
                f"Начало поездки: {format_clock(entry.trip_start)}",
                f"Заказов в поездке: {entry.trip_orders_count}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def _format_threshold(seconds: int) -> str:
    minutes = max(1, seconds // 60)
    return f"{minutes} мин."

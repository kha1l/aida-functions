"""Общий клиент для суточных рейтингов Customer Rating API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from functions.shared.manual_report_runtime import to_int


@dataclass(frozen=True, slots=True)
class CustomerRatings:
    """Средние оценки и количество оценённых заказов по каналам."""

    avg_dine_in_order_rate: float
    avg_delivery_order_rate: float
    dine_in_rate_count: int
    delivery_rate_count: int


async def fetch_customer_ratings(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    report_day: date,
) -> tuple[Optional[CustomerRatings], Optional[str], Optional[int]]:
    """Получает рейтинг точки за один завершённый локальный день."""
    result = await dodo_api.get(
        account_id,
        "https://api.dodois.io/customer-feedback/customer-ratings",
        params={
            "units": unit_uuid,
            "from": report_day.isoformat(),
            "to": report_day.isoformat(),
        },
    )
    if not result.ok:
        return None, result.error_html, result.status

    data = result.data if isinstance(result.data, dict) else {}
    rows = data.get("customerRatings", []) or []
    normalized_unit_uuid = unit_uuid.casefold()
    row = next(
        (
            item
            for item in rows
            if isinstance(item, dict)
            and str(item.get("unitId") or "").casefold() == normalized_unit_uuid
        ),
        {},
    )

    return CustomerRatings(
        avg_dine_in_order_rate=_to_float(row.get("avgDineInOrderRate")),
        avg_delivery_order_rate=_to_float(row.get("avgDeliveryOrderRate")),
        dine_in_rate_count=to_int(row.get("dineInRateCount")) or 0,
        delivery_rate_count=to_int(row.get("deliveryRateCount")) or 0,
    ), None, 200


def format_customer_rating(value: float, count: int) -> str:
    """Форматирует среднюю оценку в виде `4,7 ⭐️ (141)`."""
    rating = f"{value:.1f}".replace(".", ",")
    return f"{rating} ⭐️ ({count})"


def _to_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0

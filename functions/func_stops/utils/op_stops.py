"""Регулярный job по стопам продаж, ингредиентов и секторов."""

from __future__ import annotations

import asyncio
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

from dagster import OpExecutionContext

from database.models import Unit
from database.requests import select_report_subscriptions_many
from functions.func_stationary.utils.manual_report_common import parse_dodo_datetime
from functions.shared.report_job_runtime import (
    ReportJobSnapshot,
    load_report_job_snapshot,
    log_auth_error,
    log_final_unit_error,
    publish_report_to_subscribers,
    report_job_runtime,
)
from functions.shared.unit_time import build_previous_local_minute_window

STOP_POSTS = (
    "stops_rest",
    "stops_ings",
    "stops_key_ings",
    "stops_sector",
)
MAX_CONCURRENT_UNITS = 15
WINDOW_MINUTES = 5
DATA_LAG_MINUTES = 5
SECTOR_LOOKBACK_HOURS = 24

CHANNEL_LABELS = {
    "Takeaway": "🚶 Самовывоз",
    "Delivery": "🚗 Доставка",
    "Dine-in": "🛎 Ресторан",
}

KEY_INGREDIENTS = {
    "Тесто 25",
    "Тесто 30",
    "Тесто 35",
    "Сыр моцарелла",
    "Пицца-соус",
    "Соус Альфредо",
    "Бекон(слайс)",
    "Ветчина",
    "Говядина",
    "Картофель соломкой",
    "Кофе зерно",
    "Лук красный",
    "Мороженое",
    "Молоко",
    "Салат свежий",
    "Салями Пепперони",
    "Салями Чоризо",
    "Перец свежий",
    "Соус МаксТейсти(Бургер)",
    "Соус Барбекю",
    "Соус Чесночный ранч",
    "Томаты свежие",
    "Цыпленок филе",
    "Чеснок сухой",
    "Шампиньоны свежие",
    "Тортилья пшеничная 25см",
    "Добрый Кола 0,33",
    "Добрый Кола 0,5",
    "Маффин Три шоколада",
    "Соус Сырный порционный",
    "Чизкейк Нью-Йорк",
    "Коробка 25",
    "Коробка 30",
    "Коробка 35",
    "Стакан для кофе 250мл",
    "Стакан для кофе 300мл",
    "Стакан для кофе 400мл",
}


@dataclass(frozen=True, slots=True)
class ChannelStopEvent:
    """Одна запись стопа/перенаправления по каналу продаж."""

    sales_channel_name: str
    reason: str
    stop_type: str
    started_at: Optional[datetime]
    ended_at: Optional[datetime]


@dataclass(frozen=True, slots=True)
class IngredientStopEvent:
    """Одна запись стопа ингредиента."""

    ingredient_name: str
    reason: str
    started_at: Optional[datetime]
    ended_at: Optional[datetime]


@dataclass(frozen=True, slots=True)
class SectorStopEvent:
    """Одна запись стопа сектора доставки."""

    sector_name: str
    is_sub_sector: bool
    started_at: Optional[datetime]
    ended_at: Optional[datetime]


async def run_stops(context: OpExecutionContext) -> None:
    """Собирает все типы стопов за предыдущее завершенное 5-минутное окно."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api

    snapshot = await load_report_job_snapshot(
        db,
        load_subscriptions=lambda db_: select_report_subscriptions_many(db_, STOP_POSTS),
    )
    if not snapshot.subscriptions:
        context.log.info("stops_op: нет подписок")
        return

    dodo_api.prime_account_token_cache(snapshot.account_auth_map)

    # Один job обслуживает сразу 4 post, поэтому заранее строим индексы:
    # кто на что подписан и какие типы отчетов нужны по каждой точке.
    subscribers_by_unit_post = _group_subscribers_by_unit_post(snapshot)
    requested_posts_by_unit = _group_posts_by_unit(snapshot)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UNITS)
    publish_lock = asyncio.Lock()

    async with report_job_runtime(dodo_api, rabbitmq):
        async def process_unit(unit_uuid: str) -> None:
            async with semaphore:
                await _process_unit(
                    context=context,
                    dodo_api=dodo_api,
                    rabbitmq=rabbitmq,
                    snapshot=snapshot,
                    subscribers_by_unit_post=subscribers_by_unit_post,
                    requested_posts=requested_posts_by_unit.get(unit_uuid, set()),
                    unit_uuid=unit_uuid,
                    publish_lock=publish_lock,
                )

        await asyncio.gather(*(process_unit(unit_uuid) for unit_uuid in sorted(requested_posts_by_unit.keys())))


async def _process_unit(
    *,
    context: OpExecutionContext,
    dodo_api,
    rabbitmq,
    snapshot: ReportJobSnapshot,
    subscribers_by_unit_post: Dict[tuple[str, str], list],
    requested_posts: set[str],
    unit_uuid: str,
    publish_lock: asyncio.Lock,
) -> None:
    """Собирает все нужные stop-отчеты для одной точки и рассылает подписчикам."""
    if not requested_posts:
        return

    unit = snapshot.unit_for(unit_uuid)
    if unit is None:
        context.log.warning("stops_op: unit %s не найден в БД", unit_uuid)
        return

    candidate_accounts = snapshot.candidate_accounts_for(unit_uuid)
    if not candidate_accounts:
        context.log.warning("stops_op: нет токенов для unit %s", unit_uuid)
        return

    dt_start, dt_end = _build_report_window(unit)
    collected_messages: Optional[Dict[str, str]] = None
    last_error_html: Optional[str] = None
    last_error_status: Optional[int] = None
    successful_lookup = False

    for account_id in candidate_accounts:
        collected_messages, last_error_html, last_error_status = await _collect_stop_messages(
            dodo_api=dodo_api,
            unit=unit,
            unit_uuid=unit_uuid,
            unit_name=snapshot.unit_name_for(unit_uuid),
            account_id=account_id,
            requested_posts=requested_posts,
            dt_start=dt_start,
            dt_end=dt_end,
        )
        if last_error_html is None and last_error_status == 200:
            successful_lookup = True
            break
        log_auth_error(
            context,
            log_prefix="stops_op",
            unit_uuid=unit_uuid,
            account_id=account_id,
            error_html=last_error_html,
            error_status=last_error_status,
        )

    if not successful_lookup:
        log_final_unit_error(
            context,
            log_prefix="stops_op",
            unit_uuid=unit_uuid,
            error_html=last_error_html,
            error_status=last_error_status,
        )
        return

    for post in sorted(requested_posts):
        text = (collected_messages or {}).get(post)
        if not text:
            continue
        subscribers = subscribers_by_unit_post.get((unit_uuid, post), [])
        if not subscribers:
            continue
        await publish_report_to_subscribers(
            rabbitmq,
            publish_lock=publish_lock,
            subscribers=subscribers,
            post=post,
            text=text,
            unit_uuid=unit_uuid,
            prefer_paragraph_split=True,
        )


async def _collect_stop_messages(
    *,
    dodo_api,
    unit: Unit,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    requested_posts: set[str],
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[Optional[Dict[str, str]], Optional[str], Optional[int]]:
    """Грузит нужные stop-источники для точки и собирает тексты по post."""
    # Часть post делит один и тот же API-ответ, поэтому не делаем лишних запросов:
    # `stops_ings` и `stops_key_ings` оба строятся из `stop-sales-ingredients`.
    need_channels = "stops_rest" in requested_posts
    need_ingredients = bool({"stops_ings", "stops_key_ings"} & requested_posts)
    need_sectors = "stops_sector" in requested_posts

    tasks = []
    order: List[str] = []
    if need_channels:
        order.append("channels")
        tasks.append(
            dodo_api.get(
                account_id,
                "production/stop-sales-channels",
                params=_build_query_params(unit_uuid, dt_start, dt_end),
            )
        )
    if need_ingredients:
        order.append("ingredients")
        tasks.append(
            dodo_api.get(
                account_id,
                "production/stop-sales-ingredients",
                params=_build_query_params(unit_uuid, dt_start, dt_end),
            )
        )
    if need_sectors:
        # Сектор может быть остановлен задолго до текущего окна, а возобновлен
        # только сейчас. Берем расширенный lookback и ниже фильтруем именно
        # по локальному `endedAtLocal`/`startedAtLocal`.
        sector_query_start = dt_start - timedelta(hours=SECTOR_LOOKBACK_HOURS)
        order.append("sectors")
        tasks.append(
            dodo_api.get(
                account_id,
                "delivery/stop-sales-sectors",
                params=_build_query_params(unit_uuid, sector_query_start, dt_end),
            )
        )

    responses = await asyncio.gather(*tasks)
    response_by_name = dict(zip(order, responses))

    for name in order:
        result = response_by_name[name]
        if not result.ok:
            return None, result.error_html, result.status

    channel_rows = _rows_from_result(response_by_name.get("channels"), "stopSalesBySalesChannels")
    ingredient_rows = _rows_from_result(response_by_name.get("ingredients"), "stopSalesByIngredients")
    sector_rows = _rows_from_result(response_by_name.get("sectors"), "stopSalesBySectors")

    channel_events = _parse_channel_events(channel_rows, unit)
    ingredient_events = _parse_ingredient_events(ingredient_rows, unit)
    sector_events = _parse_sector_events(sector_rows, unit)

    messages: Dict[str, str] = {}
    if need_channels:
        message = _format_channels_message(unit_name=unit_name, events=channel_events, dt_start=dt_start, dt_end=dt_end)
        if message:
            messages["stops_rest"] = message
    if "stops_ings" in requested_posts:
        message = _format_ingredients_message(
            unit_name=unit_name,
            events=ingredient_events,
            dt_start=dt_start,
            dt_end=dt_end,
        )
        if message:
            messages["stops_ings"] = message
    if "stops_key_ings" in requested_posts:
        message = _format_key_ingredients_message(
            unit_name=unit_name,
            events=ingredient_events,
            dt_start=dt_start,
            dt_end=dt_end,
        )
        if message:
            messages["stops_key_ings"] = message
    if need_sectors:
        message = _format_sectors_message(
            unit_name=unit_name,
            events=sector_events,
            dt_start=dt_start,
            dt_end=dt_end,
        )
        if message:
            messages["stops_sector"] = message

    return messages, None, 200


def _group_subscribers_by_unit_post(snapshot: ReportJobSnapshot) -> Dict[tuple[str, str], list]:
    """Готовит быстрый индекс подписчиков по паре `unit_uuid + post`."""
    grouped: Dict[tuple[str, str], list] = {}
    for sub in snapshot.subscriptions:
        if not sub.post:
            continue
        grouped.setdefault((sub.unit_uuid, sub.post), []).append(sub)
    return grouped


def _group_posts_by_unit(snapshot: ReportJobSnapshot) -> Dict[str, set[str]]:
    """Возвращает набор нужных stop-отчетов по каждой точке."""
    grouped: Dict[str, set[str]] = {}
    for sub in snapshot.subscriptions:
        if not sub.post:
            continue
        grouped.setdefault(sub.unit_uuid, set()).add(sub.post)
    return grouped


def _build_query_params(unit_uuid: str, dt_start: datetime, dt_end: datetime) -> Dict[str, str]:
    """Собирает параметры окна для Dodo API."""
    return {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _build_report_window(unit: Optional[Unit]) -> tuple[datetime, datetime]:
    """Возвращает локальное окно отчета с небольшим лагом на задержку Dodo API."""
    dt_start, dt_end = build_previous_local_minute_window(unit, minutes=WINDOW_MINUTES)
    if DATA_LAG_MINUTES <= 0:
        return dt_start, dt_end
    lag = timedelta(minutes=DATA_LAG_MINUTES)
    return dt_start - lag, dt_end - lag


def _rows_from_result(result, key: str) -> List[dict]:
    """Аккуратно достает массив записей из DodoApiResult."""
    if result is None or not isinstance(result.data, dict):
        return []
    rows = result.data.get(key)
    return rows if isinstance(rows, list) else []


def _parse_channel_events(rows: Iterable[dict], unit: Optional[Unit]) -> List[ChannelStopEvent]:
    """Нормализует стопы по каналам продаж."""
    events: List[ChannelStopEvent] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        events.append(
            ChannelStopEvent(
                sales_channel_name=str(row.get("salesChannelName") or "Неизвестный канал").strip(),
                reason=str(row.get("reason") or "Не указана").strip(),
                stop_type=str(row.get("channelStopType") or "Complete").strip() or "Complete",
                started_at=_parse_event_time(unit, row, local_key="startedAtLocal", utc_key="startedAt"),
                ended_at=_parse_event_time(unit, row, local_key="endedAtLocal", utc_key="endedAt"),
            )
        )
    return events


def _parse_ingredient_events(rows: Iterable[dict], unit: Optional[Unit]) -> List[IngredientStopEvent]:
    """Нормализует стопы по ингредиентам."""
    events: List[IngredientStopEvent] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        events.append(
            IngredientStopEvent(
                ingredient_name=str(row.get("ingredientName") or "Неизвестный ингредиент").strip(),
                reason=str(row.get("reason") or "Не указана").strip(),
                started_at=_parse_event_time(unit, row, local_key="startedAtLocal", utc_key="startedAt"),
                ended_at=_parse_event_time(unit, row, local_key="endedAtLocal", utc_key="endedAt"),
            )
        )
    return events


def _parse_sector_events(rows: Iterable[dict], unit: Optional[Unit]) -> List[SectorStopEvent]:
    """Нормализует стопы по секторам доставки."""
    events: List[SectorStopEvent] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        events.append(
            SectorStopEvent(
                sector_name=str(row.get("sectorName") or "Неизвестный сектор").strip(),
                is_sub_sector=bool(row.get("isSubSector")),
                started_at=_parse_event_time(unit, row, local_key="startedAtLocal", utc_key="startedAt"),
                ended_at=_parse_event_time(unit, row, local_key="endedAtLocal", utc_key="endedAt"),
            )
        )
    return events


def _format_channels_message(
    *,
    unit_name: str,
    events: Sequence[ChannelStopEvent],
    dt_start: datetime,
    dt_end: datetime,
) -> Optional[str]:
    """Собирает отчет по стопам и возобновлениям каналов продаж."""
    started = [event for event in events if _is_inside_window(event.started_at, dt_start, dt_end)]
    resumed = [event for event in events if _is_inside_window(event.ended_at, dt_start, dt_end)]

    sections: List[str] = []
    if started:
        lines = [f"🔴 <b>Стопы продаж в {html.escape(unit_name)}</b>", ""]
        for event in sorted(started, key=lambda item: (item.started_at or datetime.min, item.sales_channel_name)):
            is_redirection = event.stop_type == "Redirection"
            action_label = "Перенаправление продаж" if is_redirection else "Остановка продаж"
            action_time_label = "Время перенаправления" if is_redirection else "Время остановки"
            lines.extend(
                [
                    f"🔴 {action_label}",
                    f"<b>{html.escape(_channel_label(event.sales_channel_name))}</b>",
                    f"Причина: {html.escape(event.reason)}",
                    f"{action_time_label}: {_format_time(event.started_at)}",
                    "",
                ]
            )
        sections.append("\n".join(lines).strip())
        sections.append("#стопыПоКаналамПродаж")

    if resumed:
        lines = [f"🟢 <b>Возобновления продаж в {html.escape(unit_name)}</b>", ""]
        for event in sorted(resumed, key=lambda item: (item.ended_at or datetime.min, item.sales_channel_name)):
            is_redirection = event.stop_type == "Redirection"
            action_label = "Завершение перенаправления продаж" if is_redirection else "Возобновление продаж"
            lines.extend(
                [
                    f"🟢 {action_label}",
                    f"<b>{html.escape(_channel_label(event.sales_channel_name))}</b>",
                    f"Время возобновления: {_format_time(event.ended_at)}",
                    "",
                ]
            )
        sections.append("\n".join(lines).strip())
        sections.append("#возобновленияПоКаналамПродаж")
    return "\n\n".join(section for section in sections if section).strip() or None


def _format_ingredients_message(
    *,
    unit_name: str,
    events: Sequence[IngredientStopEvent],
    dt_start: datetime,
    dt_end: datetime,
) -> Optional[str]:
    """Собирает отчет по ингредиентам за окно."""
    started = [event for event in events if _is_inside_window(event.started_at, dt_start, dt_end)]
    resumed = [event for event in events if _is_inside_window(event.ended_at, dt_start, dt_end)]
    if not started and not resumed:
        return None

    sections: List[str] = []
    if started:
        lines = [f"🔴 <b>Стопы ингредиентов в {html.escape(unit_name)}</b>", ""]
        for event in sorted(started, key=lambda item: (item.started_at or datetime.min, item.ingredient_name)):
            lines.extend(
                [
                    f"<b>{html.escape(event.ingredient_name)}</b>",
                    f"Причина: {html.escape(event.reason)}",
                    f"Время остановки: {_format_time(event.started_at)}",
                    "",
                ]
            )
        sections.append("\n".join(lines).strip())
        sections.append("#стопыИнгредиентов")

    if resumed:
        lines = [f"🟢 <b>Возобновления ингредиентов в {html.escape(unit_name)}</b>", ""]
        for event in sorted(resumed, key=lambda item: (item.ended_at or datetime.min, item.ingredient_name)):
            lines.extend(
                [
                    f"<b>{html.escape(event.ingredient_name)}</b>",
                    f"Время возобновления: {_format_time(event.ended_at)}",
                    "",
                ]
            )
        sections.append("\n".join(lines).strip())
        sections.append("#возобновленияИнгредиентов")
    return "\n\n".join(section for section in sections if section).strip() or None


def _format_key_ingredients_message(
    *,
    unit_name: str,
    events: Sequence[IngredientStopEvent],
    dt_start: datetime,
    dt_end: datetime,
) -> Optional[str]:
    """Собирает отчет только по ключевым ингредиентам."""
    filtered = [
        event for event in events
        if event.ingredient_name in KEY_INGREDIENTS
        and _is_inside_window(event.started_at, dt_start, dt_end)
    ]
    if not filtered:
        return None

    lines = [f"🔴 <b>Стопы ключевых ингредиентов в {html.escape(unit_name)}</b>", ""]
    for event in sorted(filtered, key=lambda item: (item.started_at or datetime.min, item.ingredient_name)):
        lines.extend(
            [
                f"<b>{html.escape(event.ingredient_name)}</b>",
                f"Причина: {html.escape(event.reason)}",
                f"Время остановки: {_format_time(event.started_at)}",
                "",
            ]
        )
    lines.append("#стопыИнгредиентов")
    return "\n".join(lines).strip() or None


def _format_sectors_message(
    *,
    unit_name: str,
    events: Sequence[SectorStopEvent],
    dt_start: datetime,
    dt_end: datetime,
) -> Optional[str]:
    """Собирает отчет по стопам и возобновлениям секторов."""
    started = [event for event in events if _is_inside_window(event.started_at, dt_start, dt_end)]
    resumed = [event for event in events if _is_inside_window(event.ended_at, dt_start, dt_end)]
    if not started and not resumed:
        return None

    sections: List[str] = []
    if started:
        lines = [f"🔴 <b>Стопы секторов в {html.escape(unit_name)}</b>", ""]
        for event in sorted(started, key=lambda item: (item.started_at or datetime.min, item.sector_name)):
            lines.extend(_sector_lines(event, is_resume=False))
        sections.append("\n".join(lines).strip())
        sections.append("#стопыСекторов")

    if resumed:
        lines = [f"🟢 <b>Возобновления секторов в {html.escape(unit_name)}</b>", ""]
        for event in sorted(resumed, key=lambda item: (item.ended_at or datetime.min, item.sector_name)):
            lines.extend(_sector_lines(event, is_resume=True))
        sections.append("\n".join(lines).strip())
        sections.append("#возобновленияСекторов")

    return "\n\n".join(section for section in sections if section).strip() or None


def _sector_lines(event: SectorStopEvent, *, is_resume: bool) -> List[str]:
    """Возвращает готовые строки по одному сектору."""
    time_value = event.ended_at if is_resume else event.started_at
    time_label = "Время возобновления" if is_resume else "Время остановки"
    lines = [
        f"<b>{html.escape(event.sector_name)}</b>",
        f"{time_label}: {_format_time(time_value)}",
    ]
    if event.is_sub_sector:
        lines.append("Сектор создан менеджером смены")
    lines.append("")
    return lines


def _parse_event_time(
    unit: Optional[Unit],
    row: dict,
    *,
    local_key: str,
    utc_key: str,
) -> Optional[datetime]:
    """Преобразует время события в локальное naive-время точки."""
    local_dt = parse_dodo_datetime(row.get(local_key))
    if local_dt is not None:
        return local_dt

    utc_dt = parse_dodo_datetime(row.get(utc_key))
    if utc_dt is None:
        return None
    if unit is None or unit.timezone is None:
        return utc_dt
    return utc_dt + timedelta(hours=unit.timezone)


def _channel_label(value: str) -> str:
    """Возвращает человекочитаемое имя канала продаж."""
    return CHANNEL_LABELS.get(value, value)


def _is_inside_window(value: Optional[datetime], dt_start: datetime, dt_end: datetime) -> bool:
    """Проверяет, что событие попадает в предыдущее завершенное окно."""
    return value is not None and dt_start <= value < dt_end


def _format_time(value: Optional[datetime]) -> str:
    """Форматирует время события в `HH:MM:SS`."""
    return value.strftime("%H:%M:%S") if value is not None else "Неизвестно"

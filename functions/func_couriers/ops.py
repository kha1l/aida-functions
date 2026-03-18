import asyncio
import html
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from dagster import OpExecutionContext, op

from config import couriers_settings, WeatherSettings, weather_settings
from database.requests import (
    CouriersSubscription,
    load_accounts_and_tokens_bulk,
    load_units_by_uuids,
    select_account_ids_for_units,
    select_couriers_subscriptions,
)
from messaging import build_payload


@op(required_resource_keys={"db", "rabbitmq", "dodo_api"})
def couriers_op(context: OpExecutionContext):
    """Собирает отчет по курьерам и отправляет в RabbitMQ."""
    asyncio.run(_run_couriers(context))


async def _run_couriers(context: OpExecutionContext) -> None:
    """Оркестрирует сбор, агрегацию и отправку отчётов по курьерам."""
    db = context.resources.db
    rabbitmq = context.resources.rabbitmq
    dodo_api = context.resources.dodo_api
    couriers_cfg = couriers_settings()
    weather_cfg = weather_settings()

    subscriptions = await select_couriers_subscriptions(db)
    if not subscriptions:
        context.log.info("couriers_op: нет подписок")
        return

    subs_by_unit: Dict[str, List[CouriersSubscription]] = {}
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

    semaphore = asyncio.Semaphore(couriers_cfg.max_concurrent_units)
    publish_lock = asyncio.Lock()
    weather_tasks: Dict[Tuple[str, str], asyncio.Task[str]] = {}
    weather_tasks_lock = asyncio.Lock()
    weather_request_semaphore = asyncio.Semaphore(
        max(1, weather_cfg.max_concurrent_requests)
    )
    dodo_request_semaphore = asyncio.Semaphore(max(1, dodo_api.max_concurrent_requests))

    weather_timeout = aiohttp.ClientTimeout(total=weather_cfg.timeout_seconds)
    dodo_timeout = aiohttp.ClientTimeout(total=dodo_api.timeout_seconds)
    weather_connector = aiohttp.TCPConnector(
        limit=weather_cfg.max_concurrent_requests,
        limit_per_host=weather_cfg.max_concurrent_requests,
        ttl_dns_cache=300,
    )
    async with (
        aiohttp.ClientSession(
            timeout=weather_timeout,
            trust_env=True,
            connector=weather_connector,
        ) as weather_session,
        aiohttp.ClientSession(timeout=dodo_timeout, trust_env=True) as dodo_session,
    ):
        dodo_api.configure_runtime(
            session=dodo_session,
            request_semaphore=dodo_request_semaphore,
        )
        rabbitmq_open = getattr(rabbitmq, "open_async", None)
        if callable(rabbitmq_open):
            await rabbitmq_open()

        async def process_unit(unit_uuid: str) -> None:
            """Собирает и публикует отчёт для одного заведения."""
            async with semaphore:
                subscribers = subs_by_unit.get(unit_uuid, [])
                try:
                    unit = units.get(unit_uuid)
                    order_account_ids = list({acc for acc in accounts_by_unit.get(unit_uuid, [])})

                    if not subscribers:
                        return

                    candidate_accounts = candidate_accounts_by_unit.get(
                        unit_uuid,
                        [],
                    )
                    if not candidate_accounts:
                        candidate_accounts = order_account_ids

                    if not candidate_accounts:
                        context.log.warning(
                            "couriers_op: нет токенов для unit %s", unit_uuid
                        )
                        return

                    report_text: Optional[str] = None
                    last_error_html: Optional[str] = None
                    last_error_status: Optional[int] = None
                    weather_text = await _get_weather_text_cached(
                        weather_cfg=weather_cfg,
                        weather_session=weather_session,
                        latitude=unit.latitude if unit else None,
                        longitude=unit.longitude if unit else None,
                        logger=context.log,
                        unit_name=unit.name if unit and unit.name else unit_uuid,
                        weather_tasks=weather_tasks,
                        weather_tasks_lock=weather_tasks_lock,
                        weather_request_semaphore=weather_request_semaphore,
                    )

                    for account_id in candidate_accounts:
                        account, _ = account_auth_map.get(account_id, (None, None))
                        report_text, last_error_html, last_error_status = await _build_couriers_report(
                            dodo_api=dodo_api,
                            unit_uuid=unit_uuid,
                            unit_name=unit.name if unit and unit.name else unit_uuid,
                            account_id=account_id,
                            market_code=_extract_market_code(
                                account.properties if account else None
                            ),
                            weather_text=weather_text,
                        )
                        if report_text:
                            break
                        if last_error_html and last_error_status in {401, 403}:
                            context.log.warning(
                                "couriers_op: ошибка Dodo API для unit %s, account_id=%s: %s",
                                unit_uuid,
                                account_id,
                                _html_to_log_text(last_error_html),
                            )

                    if not report_text:
                        if last_error_html:
                            if last_error_status in {429, 500}:
                                context.log.error(
                                    "couriers_op: не удалось собрать отчет для unit %s после retry: %s",
                                    unit_uuid,
                                    _html_to_log_text(last_error_html),
                                )
                            elif last_error_status not in {401, 403}:
                                context.log.error(
                                    "couriers_op: не удалось собрать отчет для unit %s: %s",
                                    unit_uuid,
                                    _html_to_log_text(last_error_html),
                                )
                        else:
                            context.log.warning(
                                "couriers_op: нет данных для unit %s", unit_uuid
                            )
                        return

                    for sub in subscribers:
                        payload = build_payload(
                            chat_id=sub.chat_id,
                            topic_id=sub.topic_id,
                            post="couriers",
                            text=report_text,
                            unit_uuid=unit_uuid,
                        )
                        async with publish_lock:
                            await rabbitmq.publish_async(payload)
                except Exception as exc:  # noqa: BLE001
                    context.log.error(
                        "couriers_op: ошибка обработки unit %s: %s", unit_uuid, exc
                    )

        try:
            tasks = [process_unit(unit_uuid) for unit_uuid in unit_uuids]
            await asyncio.gather(*tasks)
        finally:
            dodo_api.reset_runtime()
            dodo_api.clear_account_token_cache()
            rabbitmq_close = getattr(rabbitmq, "close_async", None)
            if callable(rabbitmq_close):
                await rabbitmq_close()


async def _build_couriers_report(
    *,
    dodo_api,
    unit_uuid: str,
    unit_name: str,
    account_id: int,
    market_code: Optional[str],
    weather_text: str,
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    """Собирает итоговый текст отчёта по доставке для одной точки."""
    dt_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    dt_start = dt_end - timedelta(days=1)
    date_label = dt_start.strftime("%d.%m")

    params_base = {
        "units": unit_uuid,
        "from": dt_start.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    statistics = await dodo_api.get(
        account_id,
        "delivery/statistics/",
        params=params_base,
    )
    if not statistics.ok:
        return None, statistics.error_html, statistics.status

    productivity = await dodo_api.get(
        account_id,
        "production/productivity",
        params=params_base,
    )
    if not productivity.ok:
        return None, productivity.error_html, productivity.status

    trip_counts, delivery_times_by_order_id, trip_error, trip_error_status = await _fetch_courier_orders_metrics(
        dodo_api=dodo_api,
        account_id=account_id,
        unit_uuid=unit_uuid,
        params_base=params_base,
    )
    if trip_counts is None or delivery_times_by_order_id is None:
        return None, trip_error or "Не удалось получить данные о поездках", trip_error_status

    handover = await dodo_api.get(
        account_id,
        "production/orders-handover-time",
        params=params_base,
    )
    if not handover.ok:
        return None, handover.error_html, handover.status

    prod_couriers = _extract_productivity(productivity.data if isinstance(productivity.data, dict) else {})
    revenue_del, orders_del = _extract_delivery_overview(
        statistics.data if isinstance(statistics.data, dict) else {}
    )
    long_deliveries_percent = _calculate_long_deliveries_percent(
        statistics=statistics.data if isinstance(statistics.data, dict) else {},
        delivery_times_by_order_id=delivery_times_by_order_id,
        handover=handover.data if isinstance(handover.data, dict) else {},
        market_code=market_code,
    )
    delivery_text = _format_delivery(
        statistics.data if isinstance(statistics.data, dict) else {},
        trip_counts,
        long_deliveries_percent,
    )

    message = (
        f"\U0001F69A Краткий отчет по доставке за {date_label}\n\n"
        f"\U0001F355 <b>{unit_name}</b>\n"
        f"Выручка доставки: {revenue_del}\n"
        f"Заказы на доставку: {orders_del}\n\n"
        f"Заказов на курьера в час: {prod_couriers}\n"
        f"{delivery_text}\n"
        f"{weather_text}"
    )
    return message, None, 200


async def _fetch_courier_orders_metrics(
    *,
    dodo_api,
    account_id: int,
    unit_uuid: str,
    params_base: Dict[str, str],
) -> tuple[Optional[List[int]], Optional[Dict[str, int]], Optional[str], Optional[int]]:
    """Выгружает поездки курьеров постранично и считает метрики по заказам."""
    skip = 0
    take = 500
    trip_counts = [0, 0, 0]
    delivery_times_by_order_id: Dict[str, int] = {}

    while True:
        params = dict(params_base)
        params.update({"skip": skip, "take": take})
        orders = await dodo_api.get(
            account_id,
            "delivery/couriers-orders",
            params=params,
        )
        if not orders.ok:
            return None, None, orders.error_html, orders.status

        data = orders.data if isinstance(orders.data, dict) else {}
        trip_counts, delivery_times_by_order_id = _work_couriers_orders(
            data,
            trip_counts,
            delivery_times_by_order_id,
        )
        if data.get("isEndOfListReached"):
            break
        skip += take

    return trip_counts, delivery_times_by_order_id, None, 200


def _work_couriers_orders(
    request: Dict[str, object],
    counts: List[int],
    delivery_times_by_order_id: Dict[str, int],
) -> tuple[List[int], Dict[str, int]]:
    """Агрегирует число поездок и длительности доставок из ответа API."""
    for order in request.get("couriersOrders", []) or []:
        try:
            trip = int(order.get("tripOrdersCount", 0))
        except (TypeError, ValueError):
            trip = 0
        if trip <= 1:
            counts[0] += 1
        elif trip == 2:
            counts[1] += 1
        else:
            counts[2] += 1

        order_id = order.get("orderId")
        delivery_time = _to_int(order.get("deliveryTime"))
        if isinstance(order_id, str) and order_id and delivery_time is not None:
            delivery_times_by_order_id[order_id] = delivery_time

    return counts, delivery_times_by_order_id


def _html_to_log_text(value: str) -> str:
    """Очищает HTML-ошибку до плоского текста для логов."""
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _extract_productivity(request: Dict[str, object]) -> float:
    """Извлекает показатель заказов на час курьера."""
    try:
        prod = request.get("productivityStatistics", [])[0]
        return prod.get("ordersPerCourierLabourHour", 0) or 0
    except Exception:  # noqa: BLE001
        return 0


def _extract_delivery_overview(request: Dict[str, object]) -> tuple[int, int]:
    """Извлекает выручку и количество заказов на доставку."""
    stats_list = request.get("unitsStatistics", [])
    stats = stats_list[0] if stats_list else {}
    revenue_del = int(stats.get("deliverySales") or 0)
    orders_del = int(stats.get("deliveryOrdersCount") or 0)
    return revenue_del, orders_del


def _format_delivery(
    request: Dict[str, object],
    ways: List[int],
    long_deliveries_percent: float,
) -> str:
    """Форматирует текстовый блок метрик доставки."""
    stats_list = request.get("unitsStatistics", [])
    stats = stats_list[0] if stats_list else {}

    avg_del_seconds = int(stats.get("avgDeliveryOrderFulfillmentTime") or 0)
    shelf_seconds = int(stats.get("avgHeatedShelfTime") or 0)
    trip = int(stats.get("tripsCount") or 0)
    later = int(stats.get("lateOrdersCount") or 0)

    trips_duration = float(stats.get("tripsDuration") or 0)
    shifts_duration = float(stats.get("couriersShiftsDuration") or 0)
    workload = round(trips_duration / shifts_duration * 100, 2) if shifts_duration else 0

    avg_del = timedelta(seconds=avg_del_seconds)
    shelf = timedelta(seconds=shelf_seconds)

    if avg_del_seconds >= 2400:
        em_del = "\u203C"
    elif avg_del_seconds >= 2100:
        em_del = "\u2757"
    else:
        em_del = ""

    if shelf_seconds > 180:
        em_sh = "\u2757"
    else:
        em_sh = ""

    em_later = "\u2757" if later > 0 else ""

    one = ways[0]
    two = int(ways[1] / 2) if ways[1] else 0
    three = int(ways[2] / 3) if ways[2] else 0

    perc_one = int(round(one / trip * 100, 0)) if trip else 0
    perc_two = int(round(two / trip * 100, 0)) if trip else 0
    perc_three = int(round(three / trip * 100, 0)) if trip else 0

    return (
        f"Скорость доставки: {avg_del} {em_del}\n"
        f"Время на полке: {shelf} {em_sh}\n"
        f"Опоздания: {later} {em_later}\n\n"
        f"Долгие доставки: {_format_percent(long_deliveries_percent)}%\n\n"
        f"Поездки с 1 заказом: {one} ({perc_one}%)\n"
        f"Поездки с 2 заказами: {two} ({perc_two}%)\n"
        f"Поездки с 3 и более: {three} ({perc_three}%)\n"
        f"Загруженность курьеров: {workload}%\n"
    )


def _calculate_long_deliveries_percent(
    *,
    statistics: Dict[str, object],
    delivery_times_by_order_id: Dict[str, int],
    handover: Dict[str, object],
    market_code: Optional[str],
) -> float:
    """Считает долю долгих доставок относительно всех delivery orders."""
    stats_list = statistics.get("unitsStatistics", [])
    stats = stats_list[0] if stats_list else {}
    delivery_orders_count = _to_int(stats.get("deliveryOrdersCount")) or 0
    if delivery_orders_count <= 0:
        return 0.0

    threshold_seconds = _long_delivery_threshold_seconds(market_code)
    long_deliveries_count = 0
    seen_order_ids = set()

    for order in handover.get("ordersHandoverTime", []) or []:
        if not isinstance(order, dict):
            continue
        if order.get("salesChannel") != "Delivery":
            continue

        order_id = order.get("orderId")
        if not isinstance(order_id, str) or not order_id or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)

        delivery_time = delivery_times_by_order_id.get(order_id)
        if delivery_time is None:
            continue

        tracking_pending_time = _to_int(order.get("trackingPendingTime")) or 0
        cooking_time = _to_int(order.get("cookingTime")) or 0
        heated_shelf_time = _to_int(order.get("heatedShelfTime")) or 0
        meeting_time = tracking_pending_time + cooking_time + heated_shelf_time

        if delivery_time + meeting_time > threshold_seconds:
            long_deliveries_count += 1

    return round(long_deliveries_count / delivery_orders_count * 100, 2)


def _long_delivery_threshold_seconds(market_code: Optional[str]) -> int:
    """Возвращает порог долгой доставки в секундах по рынку."""
    if (market_code or "").strip().lower() == "ru":
        return 45 * 60
    return 35 * 60


def _extract_market_code(properties: Optional[str]) -> Optional[str]:
    """Извлекает код рынка из account.properties."""
    normalized = (properties or "").strip().strip("/")
    if not normalized:
        return None
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return None
    return parts[-1].lower()


def _format_percent(value: float) -> str:
    """Форматирует процент без лишних нулей в конце."""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _to_int(value: object) -> Optional[int]:
    """Безопасно преобразует произвольное значение в int."""
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


async def _build_weather_text(
    *,
    weather_cfg: WeatherSettings,
    weather_session: aiohttp.ClientSession,
    latitude: Optional[str],
    longitude: Optional[str],
    weather_request_semaphore: asyncio.Semaphore,
    logger,
    unit_name: str,
) -> str:
    """Получает и форматирует прогноз погоды для отчёта по точке."""
    if not weather_cfg.api_key or not latitude or not longitude:
        return _weather_unavailable_text("координаты или API-ключ не настроены")

    try:
        weather_data = await _fetch_weather(
            weather_session=weather_session,
            weather_cfg=weather_cfg,
            latitude=latitude,
            longitude=longitude,
            weather_request_semaphore=weather_request_semaphore,
        )
        formatted = _format_weather(weather_data)
        if formatted:
            return formatted
        return _weather_unavailable_text("погодный сервис не вернул прогноз")
    except Exception as exc:  # noqa: BLE001
        logger.warning("couriers_op: ошибка погоды для %s: %s", unit_name, exc)
        return _weather_unavailable_text("данные временно недоступны")


async def _get_weather_text_cached(
    *,
    weather_cfg: WeatherSettings,
    weather_session: aiohttp.ClientSession,
    latitude: Optional[str],
    longitude: Optional[str],
    logger,
    unit_name: str,
    weather_tasks: Dict[Tuple[str, str], asyncio.Task[str]],
    weather_tasks_lock: asyncio.Lock,
    weather_request_semaphore: asyncio.Semaphore,
) -> str:
    """Переиспользует уже запрошенный прогноз по координатам."""
    if not weather_cfg.api_key or not latitude or not longitude:
        return _weather_unavailable_text("координаты или API-ключ не настроены")

    cache_key = (latitude, longitude)
    cached_task = weather_tasks.get(cache_key)
    if cached_task is None:
        async with weather_tasks_lock:
            cached_task = weather_tasks.get(cache_key)
            if cached_task is None:
                cached_task = asyncio.create_task(
                    _build_weather_text(
                        weather_cfg=weather_cfg,
                        weather_session=weather_session,
                        latitude=latitude,
                        longitude=longitude,
                        weather_request_semaphore=weather_request_semaphore,
                        logger=logger,
                        unit_name=unit_name,
                    )
                )
                weather_tasks[cache_key] = cached_task

    return await cached_task


async def _fetch_weather(
    *,
    weather_session: aiohttp.ClientSession,
    weather_cfg: WeatherSettings,
    latitude: str,
    longitude: str,
    weather_request_semaphore: asyncio.Semaphore,
) -> Dict[str, object]:
    """Запрашивает прогноз погоды с retry на временных ошибках."""
    params = {
        "units": "metric",
        "cnt": weather_cfg.forecast_count,
        "lat": latitude,
        "lon": longitude,
        "lang": weather_cfg.lang,
        "appid": weather_cfg.api_key,
    }
    url = "https://api.openweathermap.org/data/2.5/forecast"
    retries_left = weather_cfg.retries
    attempts = 0

    while True:
        attempts += 1
        try:
            async with weather_request_semaphore:
                async with weather_session.get(url, params=params) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        return await response.json()
                    if response.status in {429, 500, 502, 503, 504} and retries_left > 0:
                        retries_left -= 1
                        await asyncio.sleep(_weather_retry_delay(attempts, response.headers))
                        continue
                    raise ValueError(f"weather status={response.status}: {response_text}")
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            if retries_left > 0:
                retries_left -= 1
                await asyncio.sleep(_weather_retry_delay(attempts))
                continue
            raise ValueError(f"weather request failed after {attempts} attempts: {exc}") from exc


def _format_weather(data: Dict[str, object]) -> str:
    """Преобразует ответ погодного API в текст для отчёта."""
    weather_entries = data.get("list", [])
    if not isinstance(weather_entries, list) or not weather_entries:
        return ""

    icons = {
        "Thunderstorm": "\U0001F5F2",
        "Drizzle": "\u2744",
        "Rain": "\u2614",
        "Snow": "\U0001F328",
        "Atmosphere": "\u26C8",
    }

    precipitation: Dict[str, List[tuple[int, int]]] = {}
    temperatures: List[float] = []

    for step in weather_entries:
        if not isinstance(step, dict):
            continue

        main = step.get("main")
        if isinstance(main, dict):
            temp = main.get("temp")
            if isinstance(temp, (int, float)):
                temperatures.append(float(temp))

        weather_list = step.get("weather")
        dt_text = step.get("dt_txt")
        if (
            not isinstance(weather_list, list)
            or not weather_list
            or not isinstance(weather_list[0], dict)
            or not isinstance(dt_text, str)
        ):
            continue

        weather = weather_list[0]
        weather_id = weather.get("id")
        weather_main = weather.get("main")
        description = weather.get("description")
        if not isinstance(weather_id, int) or weather_id >= 800:
            continue
        if not isinstance(weather_main, str) or not isinstance(description, str):
            continue

        start_hour = _extract_hour(dt_text)
        if start_hour is None:
            continue
        label = f"{icons.get(weather_main, '')} {description}".strip()
        precipitation.setdefault(label, []).append((start_hour, start_hour + 3))

    forecast_lines = ["<b>Прогноз погоды на сегодня:</b>"]
    if precipitation:
        for label, intervals in precipitation.items():
            merged = _merge_intervals(intervals)
            time_ranges = ", ".join(
                f"с {start} до {end}ч." for start, end in merged
            )
            forecast_lines.append(f"{label} {time_ranges}")
    else:
        forecast_lines.append("\U0001F31E без осадков")

    if not temperatures:
        return "\n".join(forecast_lines) + "\n"

    middle = max(1, len(temperatures) // 2)
    day_temps = temperatures[:middle]
    evening_temps = temperatures[middle:]
    day_temp = sum(day_temps) / len(day_temps) if day_temps else 0
    evening_temp = sum(evening_temps) / len(evening_temps) if evening_temps else day_temp

    temp_lines = [
        "\U0001F321 Средняя температура:",
        f"    \U0001F315 День: {round(day_temp, 1)}°C",
        f"    \U0001F311 Вечер: {round(evening_temp, 1)}°C",
    ]
    if day_temp < 0:
        temp_lines.append("")
        temp_lines.append("Одевайтесь теплее! \U0001F912")
    elif evening_temp < 0:
        temp_lines.append("")
        temp_lines.append("При выходе на улицу, не забудьте надеть шапку! \U0001F976")

    return "\n".join(forecast_lines + [""] + temp_lines) + "\n\n" + "#отчетПоКурьерам"


def _weather_retry_delay(attempt: int, headers: Optional["aiohttp.typedefs.LooseHeaders"] = None) -> float:
    """Возвращает backoff-задержку для повторного weather-запроса."""
    if headers:
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except (TypeError, ValueError):
                pass
    return min(2 ** (attempt - 1), 8)


def _weather_unavailable_text(reason: str) -> str:
    """Строит fallback-текст, если прогноз погоды недоступен."""
    return (
        "<b>Прогноз погоды на сегодня:</b>\n"
        f"Данные о погоде временно недоступны ({reason}).\n"
    )


def _extract_hour(dt_text: str) -> Optional[int]:
    """Извлекает час из строки формата OpenWeather dt_txt."""
    try:
        return int(dt_text[11:13])
    except (TypeError, ValueError):
        return None


def _merge_intervals(intervals: List[tuple[int, int]]) -> List[tuple[int, int]]:
    """Объединяет пересекающиеся временные интервалы осадков."""
    if not intervals:
        return []
    sorted_intervals = sorted(intervals)
    merged: List[tuple[int, int]] = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        current_start, current_end = merged[-1]
        if start <= current_end:
            merged[-1] = (current_start, max(current_end, end))
        else:
            merged.append((start, end))
    return merged

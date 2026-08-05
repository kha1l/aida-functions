"""Локализация готовых текстов отчетов для отправки в RabbitMQ.

Базовый язык отчетов в `aida_functions` сейчас русский. Для большинства отчетов
достаточно перевести уже собранный текст по набору стабильных меток. Это позволяет
не дублировать всю бизнес-логику формирования отчета под каждый язык.
"""

from __future__ import annotations

import re
from typing import Callable, Pattern

from .i18n import is_english


RegexReplacer = tuple[Pattern[str], str | Callable[[re.Match[str]], str]]


_COMMON_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("Период: ", "Period: "),
    ("Выручка: ", "Revenue: "),
    ("Заказы: ", "Orders: "),
    ("Номер заказа: ", "Order number: "),
    ("Время заказа: ", "Order time: "),
    ("Причина: ", "Reason: "),
    ("Не указан", "Not specified"),
    ("Не удалось определить", "Failed to resolve"),
    ("Да 🟢", "Yes 🟢"),
    ("Да 🔴", "Yes 🔴"),
    ("Нет 🟢", "No 🟢"),
    ("Нет 🔴", "No 🔴"),
    ("Отличная работа, так держать!", "Great job, keep it up!"),
    ("Не найдено.", "Not found."),
)


_POST_REPLACEMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "couriers": (
        ("Рейтинг клиента на доставку: ", "Delivery customer rating: "),
        ("🚚 Краткий отчет по доставке за ", "🚚 Delivery summary for "),
        ("Выручка доставки: ", "Delivery revenue: "),
        ("Заказы на доставку: ", "Delivery orders: "),
        ("Заказов на курьера в час: ", "Orders per courier per hour: "),
        ("Скорость доставки: ", "Delivery speed: "),
        ("Время на полке: ", "Shelf time: "),
        ("Опоздания: ", "Late orders: "),
        ("Долгие доставки: ", "Long deliveries: "),
        ("Поездки с 1 заказом: ", "Trips with 1 order: "),
        ("Поездки с 2 заказами: ", "Trips with 2 orders: "),
        ("Поездки с 3 и более: ", "Trips with 3+ orders: "),
        ("Загруженность курьеров: ", "Courier workload: "),
        ("<b>Прогноз погоды на сегодня:</b>", "<b>Weather forecast for today:</b>"),
        ("Данные о погоде временно недоступны", "Weather data is temporarily unavailable"),
        ("День: ", "Day: "),
        ("Вечер: ", "Evening: "),
        ("#отчетПоКурьерам", "#couriersReport"),
    ),
    "stationary": (
        ("Рейтинг клиента в ресторане: ", "Dine-in customer rating: "),
        ("🌇 Краткий отчет по ресторану за ", "🌇 Restaurant summary for "),
        ("Среднее время приготовления: ", "Average prep time: "),
        ("Долгие заказы: ", "Long orders: "),
        ("Заказов через приложение: ", "Orders via app: "),
        ("Заказов через киоск: ", "Orders via kiosk: "),
        ("Продажи через ресторан:", "Restaurant sales:"),
        ("Продажи через киоск:", "Kiosk sales:"),
        ("     Комбо: ", "     Combo: "),
        ("     Доп. ингредиенты: ", "     Added ingredients: "),
        ("#отчетПоРесторану", "#restaurantReport"),
    ),
    "metrics": (
        ("📊 <b>Ключевые метрики в заведении ", "📊 <b>Key metrics for "),
        ("Производительность: ", "Productivity: "),
        ("Продуктов на чел/час: ", "Products per labor hour: "),
        ("Сотрудники на смене:", "Employees on shift:"),
        ("        Кухня: ", "        Kitchen: "),
        ("        Курьеры: ", "        Couriers: "),
        ("Скорость доставки: ", "Delivery speed: "),
        ("Среднее время курьера с заказом в пути: ", "Average courier trip time: "),
        ("Время на полке: ", "Shelf time: "),
        ("Долгие доставки: ", "Long deliveries: "),
        ("Заказов на курьера/час: ", "Orders per courier per hour: "),
        ("Сертификаты: ", "Certificates: "),
        ("Время приготовления в ресторан: ", "Prep time for dine-in: "),
        ("Время приготовления на доставку: ", "Prep time for delivery: "),
        ("Процент долгих приготовлений: ", "Long prep share: "),
        ("#метрики", "#metrics"),
    ),
    "revenue": (
        ("📊 <b>Выручка по сети:</b>", "📊 <b>Revenue by network:</b>"),
        ("Итого:", "Total:"),
        ("#выручкаПоСети", "#networkRevenue"),
    ),
    "stock": (
        ("🧀  Складские остатки на ", "🧀  Stock balances at "),
        ("🧀 Складские остатки на ", "🧀 Stock balances at "),
        ("⚠️  Некорректные остатки:", "⚠️  Invalid balances:"),
        ("‼️  Хватит менее чем на день:", "‼️  Enough for less than a day:"),
        ("❕ Хватит на 2 дня:", "❕ Enough for 2 days:"),
        ("❔ Подозрительные данные Dodo IS:", "❔ Suspicious Dodo IS data:"),
        ("✅ Критичных позиций не найдено", "✅ No critical stock items found"),
        (" осталось ", " left "),
        (" - некорректный остаток", " - invalid balance"),
        (" - расход по dodois меньше нуля", " - Dodo IS consumption is below zero"),
        (" - хватит менее чем на день", " - enough for less than a day"),
        (" - хватит на ", " - enough for "),
        (" дня", " days"),
        (" день", " day"),
        ("#складскиеОстатки", "#warehouseBalances"),
    ),
    "refusal": (
        ("⛔ <b>Отмененные заказы в ", "⛔ <b>Cancelled orders in "),
        ("Время заказа: ", "Order time: "),
        ("Продукты:", "Products:"),
        ("- Не удалось определить состав", "- Failed to resolve the composition"),
        ("Стоимость: ", "Amount: "),
        ("#отказы", "#refusals"),
    ),
    "defect": (
        ("🧪 <b>Брак в ", "🧪 <b>Defects in "),
        ("Брака за час: ", "Defects this hour: "),
        ("    Стоимость: ", "    Amount: "),
        ("    Время: ", "    Time: "),
        ("    Добавленный ингредиент: ", "    Added ingredient: "),
        ("    Пицца-половинка: ", "    Pizza half: "),
        ("#брак", "#defects"),
    ),
    "writeoff": (
        ("🗑 <b>Списания за ", "🗑 <b>Write-offs for "),
        ("<b>По причинам списания:</b>", "<b>By write-off reason:</b>"),
        ("<b>Самое дорогое списание:</b>", "<b>Most expensive write-off:</b>"),
        ("<b>Самое частое списание за 7 дней:</b>", "<b>Most frequent write-off for 7 days:</b>"),
        ("        Количество: ", "        Quantity: "),
        ("        Стоимость: ", "        Amount: "),
        ("        % от выручки: ", "        % of revenue: "),
        ("        Списаний: ", "        Write-offs: "),
        ("Списания по времени с витрины", "Showcase time write-offs"),
        ("Списания кусочков", "Slice write-offs"),
        ("#списания", "#writeoffs"),
    ),
    "tickets": (
        ("📬 <b>Тикеты клиентов в ", "📬 <b>Customer tickets in "),
        ("Оценка: ", "Rating: "),
        ("Тип заказа: ", "Order type: "),
        ("Канал обращения: ", "Contact channel: "),
        ("Курьер: ", "Courier: "),
        ("Комментарий: ", "Comment: "),
        ("Дата и время заказа: ", "Order date and time: "),
        ("Тикет Pyrus: ", "Pyrus ticket: "),
        (">открыть<", ">open<"),
        ("#тикеты", "#tickets"),
    ),
    "tracking": (
        ("🛰 <b>Трекинг за ", "🛰 <b>Tracking for "),
        (" в заведении ", " at "),
        ("⬆️ Топ-5 по количеству:", "⬆️ Top 5 by quantity:"),
        ("⬇️ Tоп-5 по самому долгому времени:", "⬇️ Top 5 by longest duration:"),
        ("        Приготовлено: ", "        Produced: "),
        ("        Среднее время: ", "        Average time: "),
        (" шт.", " pcs."),
        ("#трекинг", "#tracking"),
    ),
    "stops_rest": (
        ("🔴 <b>Стопы продаж в ", "🔴 <b>Sales stops in "),
        ("🟢 <b>Возобновления продаж в ", "🟢 <b>Sales resumptions in "),
        ("🔴 Остановка продаж", "🔴 Sales stop"),
        ("🔴 Перенаправление продаж", "🔴 Sales redirection"),
        ("🟢 Возобновление продаж", "🟢 Sales resumed"),
        ("🟢 Завершение перенаправления продаж", "🟢 Sales redirection ended"),
        ("Время остановки: ", "Stop time: "),
        ("Время возобновления: ", "Resume time: "),
    ),
    "stops_ings": (
        ("🔴 <b>Стопы ингредиентов в ", "🔴 <b>Ingredient stops in "),
        ("🟢 <b>Возобновления ингредиентов в ", "🟢 <b>Ingredient resumptions in "),
        ("Время остановки: ", "Stop time: "),
        ("Время возобновления: ", "Resume time: "),
    ),
    "stops_key_ings": (
        ("🔴 <b>Стопы ключевых ингредиентов в ", "🔴 <b>Key ingredient stops in "),
        ("Время остановки: ", "Stop time: "),
    ),
    "stops_sector": (
        ("🔴 <b>Стопы секторов в ", "🔴 <b>Sector stops in "),
        ("🟢 <b>Возобновления секторов в ", "🟢 <b>Sector resumptions in "),
        ("Время остановки: ", "Stop time: "),
        ("Время возобновления: ", "Resume time: "),
    ),
    "certificates": (
        ("📑 <b>Сертификаты за ", "📑 <b>Certificates for "),
        (" в заведении ", " at "),
        ("Сертификатов не найдено. Отличная работа, так держать!", "No certificates found. Great job, keep it up!"),
        ("🚴 Курьер: ", "🚴 Courier: "),
        ("Время готовности заказа: ", "Order ready time: "),
        ("Время заказа на полке: ", "Shelf time: "),
        ("Время сборки заказа: ", "Assembly time: "),
        ("Количество курьеров в очереди: ", "Couriers in queue: "),
        ("Начало поездки: ", "Trip start: "),
        ("Поездка дольше прогноза: ", "Trip longer than forecast: "),
        ("Заказов в поездке: ", "Orders in trip: "),
    ),
    "waiting": (
        ("💤 <b>Ожидания за ", "💤 <b>Waiting times for "),
        (" в заведении ", " at "),
        ("Порог ожидания: больше ", "Waiting threshold: more than "),
        ("Ожиданий дольше ", "No waits longer than "),
        (" не найдено. Отличная работа, так держать!", " found. Great job, keep it up!"),
        ("⛷ Курьер: ", "⛷ Courier: "),
        ("Время готовности заказа: ", "Order ready time: "),
        ("Время сборки заказа: ", "Assembly time: "),
        ("Время заказа на полке: ", "Shelf time: "),
        ("Количество курьеров в очереди: ", "Couriers in queue: "),
        ("Начало поездки: ", "Trip start: "),
        ("Заказов в поездке: ", "Orders in trip: "),
        (" мин.", " min."),
        (" мин", " min"),
    ),
    "sectors": (
        ("🗺 <b>Сектора доставки за ", "🗺 <b>Delivery sectors for "),
        (" в заведении ", " at "),
        ("    Заказы: ", "    Orders: "),
        ("    Выручка: ", "    Revenue: "),
        ("    Стопы: ", "    Stops: "),
    ),
    "longers": (
        ("🛵 <b>Долгие доставки за ", "🛵 <b>Long deliveries for "),
        (" в заведении ", " at "),
        ("Порог: больше ", "Threshold: more than "),
        ("Долгих доставок не найдено. Отличная работа, так держать!", "No long deliveries found. Great job, keep it up!"),
        ("Курьер: ", "Courier: "),
        ("Общее время доставки: ", "Total delivery time: "),
        ("Фактическое время доставки: ", "Actual delivery time: "),
        ("Заказов в поездке: ", "Orders in trip: "),
        ("Некорректная доставка: ", "Incorrect delivery: "),
        ("Сектор: ", "Sector: "),
        ("Дольше прогноза: ", "Longer than forecast: "),
    ),
    "later_rest": (
        ("⏱ <b>Долгие заказы за ", "⏱ <b>Long orders for "),
        (" в заведении ", " at "),
        (
            "Для Drinkit показываем только заказы с общим временем > 6 минут.",
            "For Drinkit, only orders with total time > 6 minutes are shown.",
        ),
        ("Долгих заказов не найдено. Отличная работа, так держать!", "No long orders found. Great job, keep it up!"),
        ("Время на трекинге: ", "Tracking time: "),
        ("Время приготовления: ", "Cooking time: "),
        ("Источник заказа: ", "Order source: "),
        ("Общее время: ", "Total time: "),
    ),
    "assembly_rest": (
        ("🧳 <b>Долгая сборка заказа за ", "🧳 <b>Long order assembly for "),
        (" в заведении ", " at "),
        ("Заказов со сборкой дольше 1 минуты не найдено. Отличная работа, так держать!", "No orders with assembly longer than 1 minute were found. Great job, keep it up!"),
        ("Источник заказа: ", "Order source: "),
        ("Время ожидания сборки: ", "Assembly waiting time: "),
    ),
    "drink_rest": (
        ("🍺 <b>Долгие напитки за ", "🍺 <b>Long drinks for "),
        (" в заведении ", " at "),
        ("Порог времени: больше ", "Time threshold: more than "),
        ("Название продукта: ", "Product name: "),
        ("Среднее время на станции: ", "Average station time: "),
        ("Общее среднее время: ", "Total average time: "),
    ),
}


_COMMON_REGEX_REPLACEMENTS: tuple[RegexReplacer, ...] = (
    (re.compile(r"(\d+)ч\.\s*(\d+)м\."), r"\1h \2m"),
    (re.compile(r"(\d+)\sм\."), r"\1m"),
)


def translate_report_text(post: str, language: object, text: str) -> str:
    """Переводит уже собранный текст отчета на английский язык для чата пользователя."""
    if not is_english(language):
        return text

    translated = text
    for source, target in _POST_REPLACEMENTS.get(post, ()):
        translated = translated.replace(source, target)
    for source, target in _COMMON_REPLACEMENTS:
        translated = translated.replace(source, target)
    for pattern, replacement in _COMMON_REGEX_REPLACEMENTS:
        translated = pattern.sub(replacement, translated)
    return translated

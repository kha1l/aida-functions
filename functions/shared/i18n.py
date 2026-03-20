"""Простые i18n-helper'ы для формирования текстов отчетов в `aida_functions`."""

from __future__ import annotations


DEFAULT_REPORT_LANGUAGE = "ru"


def normalize_language(value: object) -> str:
    """Нормализует код языка до `ru` или `en`."""
    if not isinstance(value, str):
        return DEFAULT_REPORT_LANGUAGE
    normalized = value.strip().lower()
    if normalized.startswith("en"):
        return "en"
    return DEFAULT_REPORT_LANGUAGE


def is_english(language: object) -> bool:
    """Возвращает `True`, если отчет нужно формировать на английском."""
    return normalize_language(language) == "en"


def pick(language: object, ru: str, en: str) -> str:
    """Возвращает строку на нужном языке."""
    return en if is_english(language) else ru


def yes_no(language: object, value: bool) -> str:
    """Возвращает короткий `Да/Нет` или `Yes/No`."""
    if value:
        return pick(language, "Да", "Yes")
    return pick(language, "Нет", "No")


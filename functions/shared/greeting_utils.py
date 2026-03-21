"""Вспомогательные функции для нормализации коротких AI-поздравлений."""

from __future__ import annotations

import html
import re
from typing import Optional


_SENTENCE_RE = re.compile(r"[^.!?]+[.!?]")


def normalize_single_sentence_greeting_html(value: str) -> Optional[str]:
    """Возвращает HTML-курсив, если ответ похож на одно короткое предложение."""
    cleaned = value.replace("<i>", " ").replace("</i>", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    sentences = [sentence.strip() for sentence in _SENTENCE_RE.findall(cleaned) if sentence.strip()]
    if not sentences:
        return None

    if len(sentences) == 1:
        return f"<i>{html.escape(sentences[0])}</i>"

    # Если модель вернула несколько предложений, выбираем наиболее информативное:
    # - предпочитаем предложения длиннее порога (обычно это полезный текст, а не просто "Имя, ...!")
    # - если таких нет, берём самое длинное предложение
    LONG_THRESHOLD = 20
    long_candidates = [s for s in sentences if len(s) >= LONG_THRESHOLD]
    if long_candidates:
        chosen = max(long_candidates, key=len)
    else:
        chosen = max(sentences, key=len)

    return f"<i>{html.escape(chosen)}</i>"

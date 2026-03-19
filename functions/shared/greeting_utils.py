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
    if len(sentences) != 1:
        return None
    return f"<i>{html.escape(sentences[0])}</i>"

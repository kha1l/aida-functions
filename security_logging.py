from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Any


_EMAIL = re.compile(
    r"(?i)[a-z0-9.!#$%&'*+/=?^_\x60{|}~-]+@"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+"
)
_PHONE = re.compile(
    r"(?:\+7|8)[ \t()\-]*[0-9]{3}[ \t()\-]*"
    r"[0-9]{3}[ \t\-]*[0-9]{2}[ \t\-]*[0-9]{2}"
)
_LONG_IDENTIFIER = re.compile(r"(?<![A-Za-z0-9])-?\d{6,}(?![A-Za-z0-9])")
_AUTHORIZATION = re.compile(
    r"(?i)\b(authorization|proxy-authorization)\b\s*[:=]\s*"
    r"(?:bearer\s+)?[^\s,;]+"
)
_QUOTED_CREDENTIAL = re.compile(
    r"""(?ix)
    (?P<prefix>
        ["']?
        (?:authorization|proxy-authorization|cookie|set-cookie|
           access_token|refresh_token|id_token|api_key|token|secret|password)
        ["']?
        \s*[:=]\s*
    )
    (?P<value>
        "(?:\\.|[^"\\])*"
        |
        '(?:\\.|[^'\\])*'
        |
        (?:bearer\s+)?[^\s,;}]+
    )
    """
)
_CREDENTIAL = re.compile(
    r"(?i)\b(authorization|proxy-authorization|cookie|set-cookie|"
    r"access_token|refresh_token|id_token|api_key|token|secret|password)"
    r"\b\s*[:=]\s*[^\s,;]+"
)


def _redact_quoted_credential(match: re.Match[str]) -> str:
    raw_value = match.group("value")
    quote = raw_value[0] if raw_value.startswith(("'", '"')) else ""
    return f"{match.group('prefix')}{quote}[REDACTED]{quote}"


def pseudonymize(value: Any) -> str | None:
    """Возвращает versioned HMAC или None, если ключ псевдонимизации не задан."""

    normalized = str(value or "").strip()
    key = os.getenv("SECURITY_PSEUDONYM_KEY", "").strip()
    if not normalized or not key:
        return None
    version = os.getenv("SECURITY_PSEUDONYM_KEY_VERSION", "v1").strip() or "v1"
    digest = hmac.new(
        key.encode("utf-8"),
        normalized.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{version}:{digest}"


def safe_identifier(value: Any) -> str:
    """Не позволяет вернуть исходный идентификатор даже без HMAC-ключа."""

    return pseudonymize(value) or "[REDACTED_ID]"


def redact_text(value: Any) -> str:
    """Удаляет распространённые секреты и ПДн из диагностического текста."""

    text = str(value or "").replace("\r", " ").replace("\n", " ")
    text = _QUOTED_CREDENTIAL.sub(_redact_quoted_credential, text)
    text = _AUTHORIZATION.sub(
        lambda match: f"{match.group(1)}=[REDACTED]",
        text,
    )
    text = _CREDENTIAL.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _EMAIL.sub("[REDACTED_EMAIL]", text)
    text = _PHONE.sub("[REDACTED_PHONE]", text)
    return _LONG_IDENTIFIER.sub(
        lambda match: pseudonymize(match.group(0)) or "[REDACTED_ID]",
        text,
    )

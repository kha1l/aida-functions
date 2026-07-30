"""Dagster-resource и async-клиент Zveno AI для генерации поздравлений."""

from __future__ import annotations

import asyncio
import html
import json
import re
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional
from urllib.parse import urljoin

import aiohttp
from dagster import Field, InitResourceContext, IntSource, StringSource, resource

from config import zveno_settings
from security_logging import safe_identifier


@dataclass(frozen=True)
class ZvenoResult:
    """Результат генерации текста через Zveno."""

    ok: bool
    text: Optional[str]
    error_html: Optional[str]
    status: Optional[int]
    model: Optional[str]


class ZvenoClient:
    """Небольшой async-клиент Zveno с безопасным fallback, как в reminder."""

    def __init__(
        self,
        *,
        api_key: Optional[str],
        base_url: str,
        default_model: str,
        timeout_seconds: int = 120,
        retries: int = 3,
        max_concurrent_requests: int = 2,
        min_request_interval_seconds: float = 0.0,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self._shared_session: Optional[aiohttp.ClientSession] = None
        self._request_semaphore: Optional[asyncio.Semaphore] = None
        self._default_request_semaphore = asyncio.Semaphore(max(1, max_concurrent_requests))
        self._min_request_interval_seconds = max(0.0, float(min_request_interval_seconds))
        self._rate_limit_lock = asyncio.Lock()
        self._next_request_at = 0.0

    async def generate_greeting(
        self,
        *,
        employee_name: str,
        occasion: str,
        language: str = "ru",
        age: Optional[int] = None,
        details: Optional[Iterable[str]] = None,
        tone: str = "теплый, уважительный и живой",
        model: Optional[str] = None,
    ) -> ZvenoResult:
        """Генерирует готовое короткое поздравление сотруднику."""
        request_model = model or self.default_model
        if not self.api_key:
            return _result_error(
                reason="Не задан ZVENOAI_API_KEY",
                url=_chat_completions_url(self.base_url),
                model=request_model,
            )

        detail_lines = [item.strip() for item in (details or []) if item and item.strip()]
        english = str(language).lower().startswith("en")
        if english:
            prompt_lines = [
                f"Write a greeting for employee {employee_name}.",
                f"Occasion: {occasion}.",
            ]
        else:
            prompt_lines = [
                f"Напиши поздравление для сотрудника {employee_name}.",
                f"Повод: {occasion}.",
            ]
        if age is not None:
            prompt_lines.append(f"{'Age' if english else 'Возраст'}: {age}.")
        prompt_lines.append(f"{'Tone' if english else 'Тон'}: {tone}.")
        if detail_lines:
            prompt_lines.append("Additional details:" if english else "Дополнительные детали:")
            prompt_lines.extend(f"- {item}" for item in detail_lines)

        payload = {
            "model": request_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You help an HR team write short, warm and personal employee greetings in English. "
                        "Avoid cliches, bureaucratic phrasing and overly pompous tone. "
                        "Return a message that is ready to send to the employee."
                        if english
                        else
                        "Ты помогаешь HR-команде писать короткие, доброжелательные и "
                        "персональные поздравления сотрудникам на русском языке. "
                        "Избегай канцелярита, штампов и слишком пафосного тона. "
                        "Текст должен быть готов к отправке сотруднику."
                    ),
                },
                {"role": "user", "content": "\n".join(prompt_lines)},
            ],
            "temperature": 0.8,
        }
        return await self._request_greeting(payload=payload, model=request_model)

    async def generate_greeting_or_fallback(
        self,
        *,
        employee_name: str,
        occasion: str,
        fallback: Callable[[], str],
        normalize: Callable[[str], Optional[str]],
        log_warning: Callable[..., None],
        log_prefix: str,
        subject: str,
        language: str = "ru",
        age: Optional[int] = None,
        details: Optional[Iterable[str]] = None,
        tone: str = "теплый, уважительный и живой",
        model: Optional[str] = None,
    ) -> str:
        """Генерирует поздравление и безопасно возвращает локальный fallback при любой AI-ошибке."""
        result = await self.generate_greeting(
            employee_name=employee_name,
            occasion=occasion,
            language=language,
            age=age,
            details=details,
            tone=tone,
            model=model,
        )
        if not result.ok or not result.text:
            if result.error_html:
                log_warning(
                    "%s: Zveno не сгенерировал поздравление для subject_hash=%s, status=%s",
                    log_prefix,
                    safe_identifier(subject),
                    result.status,
                )
            return fallback()

        greeting = normalize(result.text)
        if greeting is None:
            log_warning(
                "%s: Zveno вернул неожиданный формат поздравления для subject_hash=%s",
                log_prefix,
                safe_identifier(subject),
            )
            return fallback()
        return greeting

    def configure_runtime(
        self,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        request_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        self._shared_session = session
        self._request_semaphore = request_semaphore

    def reset_runtime(self) -> None:
        self._shared_session = None
        self._request_semaphore = None

    async def _request_greeting(self, *, payload: Dict[str, object], model: str) -> ZvenoResult:
        url = _chat_completions_url(self.base_url)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        attempts = 0
        retries_left = self.retries

        if self._shared_session is not None:
            session = self._shared_session
            close_session = False
        else:
            session = aiohttp.ClientSession(timeout=timeout, trust_env=True)
            close_session = True

        try:
            while True:
                attempts += 1
                response_text: Optional[str] = None
                response_headers: Dict[str, str] = {}
                status: Optional[int] = None
                try:
                    semaphore_context = self._request_semaphore or self._default_request_semaphore
                    async with semaphore_context:
                        await self._wait_for_request_slot()
                        async with session.post(
                            url,
                            json=payload,
                            headers=_build_headers(api_key=self.api_key),
                        ) as response:
                            status = response.status
                            response_headers = dict(response.headers)
                            response_text = await response.text()
                            self._remember_rate_limit(response_headers)
                except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                    if retries_left > 0:
                        retries_left -= 1
                        await asyncio.sleep(_retry_delay(self.retries - retries_left))
                        continue
                    return _result_error(
                        reason="Сетевая ошибка или таймаут",
                        url=url,
                        model=model,
                        exception=exc,
                        attempts=attempts,
                    )

                if status in {429, 500, 502, 503, 504} and retries_left > 0:
                    retries_left -= 1
                    await asyncio.sleep(
                        _retry_delay(
                            self.retries - retries_left,
                            status=status,
                            headers=response_headers,
                        )
                    )
                    continue

                if status == 200:
                    try:
                        data = json.loads(response_text or "{}")
                    except Exception as exc:  # noqa: BLE001
                        return _result_error(
                            reason="Ответ Zveno не является JSON",
                            url=url,
                            model=model,
                            status=status,
                            response_text=response_text,
                            exception=exc,
                            attempts=attempts,
                            request_id=_extract_request_id(response_headers),
                        )

                    text = _extract_text(data)
                    if text:
                        return ZvenoResult(
                            ok=True,
                            text=text,
                            error_html=None,
                            status=status,
                            model=_extract_model(data) or model,
                        )

                    return _result_error(
                        reason="Zveno не вернул текст поздравления",
                        url=url,
                        model=model,
                        status=status,
                        response_text=response_text,
                        attempts=attempts,
                        request_id=_extract_request_id(response_headers),
                    )

                return _result_error(
                    reason=_extract_error_reason(response_text) or "Неуспешный статус ответа",
                    url=url,
                    model=model,
                    status=status,
                    response_text=response_text,
                    attempts=attempts,
                    request_id=_extract_request_id(response_headers),
                )
        finally:
            if close_session:
                await session.close()

    async def _wait_for_request_slot(self) -> None:
        if self._min_request_interval_seconds <= 0:
            return
        async with self._rate_limit_lock:
            now = time.monotonic()
            if self._next_request_at > now:
                await asyncio.sleep(self._next_request_at - now)
            self._next_request_at = time.monotonic() + self._min_request_interval_seconds

    def _remember_rate_limit(self, headers: Dict[str, str]) -> None:
        reset_at = _rate_limit_reset_monotonic(headers)
        if reset_at is not None:
            self._next_request_at = max(self._next_request_at, reset_at)


@resource(
    config_schema={
        "api_key": Field(StringSource, is_required=False),
        "base_url": Field(StringSource, is_required=False),
        "model": Field(StringSource, is_required=False),
        "timeout_seconds": Field(IntSource, is_required=False),
        "retries": Field(IntSource, is_required=False),
        "max_concurrent_requests": Field(IntSource, is_required=False),
        "min_request_interval_seconds": Field(IntSource, is_required=False),
    }
)
def zveno_resource(context: InitResourceContext) -> ZvenoClient:
    """Dagster-resource Zveno AI с env-алиасами как в reminder."""
    cfg = zveno_settings()
    overrides: Dict[str, str | int | None] = context.resource_config or {}
    return ZvenoClient(
        api_key=overrides.get("api_key", cfg.api_key),
        base_url=str(overrides.get("base_url", cfg.base_url)),
        default_model=str(overrides.get("model", cfg.model)),
        timeout_seconds=int(overrides.get("timeout_seconds", cfg.timeout_seconds)),
        retries=int(overrides.get("retries", cfg.retries)),
        max_concurrent_requests=int(
            overrides.get("max_concurrent_requests", cfg.max_concurrent_requests)
        ),
        min_request_interval_seconds=float(
            overrides.get("min_request_interval_seconds", cfg.min_request_interval_seconds)
        ),
    )


def _chat_completions_url(base_url: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", "chat/completions")


def _build_headers(*, api_key: Optional[str]) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key or ''}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _extract_text(data: Dict[str, object]) -> Optional[str]:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        return None
    message = first_choice.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content.strip() or None
    if isinstance(content, list):
        chunks: list[str] = []
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "text":
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
        return "\n".join(chunks).strip() or None
    return None


def _extract_model(data: Dict[str, object]) -> Optional[str]:
    model = data.get("model")
    return model if isinstance(model, str) and model.strip() else None


def _extract_error_reason(response_text: Optional[str]) -> Optional[str]:
    if not response_text:
        return None
    try:
        payload = json.loads(response_text)
    except Exception:  # noqa: BLE001
        return _truncate(response_text)
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    return _truncate(response_text)


def _retry_delay(
    attempt: int,
    *,
    status: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
) -> float:
    if status == 429 and headers:
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
        reset_delay = _rate_limit_reset_delay(headers)
        if reset_delay is not None:
            return max(reset_delay, 1.0)
    return min(2 ** (attempt - 1), 8)


def _rate_limit_reset_delay(headers: Dict[str, str]) -> Optional[float]:
    reset_value = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
    if not reset_value:
        return None
    try:
        reset_raw = float(reset_value)
    except ValueError:
        return None
    reset_epoch = reset_raw / 1000 if reset_raw > 10_000_000_000 else reset_raw
    return max(reset_epoch - time.time(), 0.0)


def _rate_limit_reset_monotonic(headers: Dict[str, str]) -> Optional[float]:
    delay = _rate_limit_reset_delay(headers)
    if delay is None:
        return None
    return time.monotonic() + delay


def _extract_request_id(headers: Dict[str, str]) -> Optional[str]:
    for header in ("x-request-id", "request-id"):
        value = headers.get(header)
        if value:
            return value
    return None


def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)]}…"


def _html_to_log_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    return " ".join(cleaned.split())


def _format_error_html(
    *,
    url: str,
    model: Optional[str],
    status: Optional[int] = None,
    reason: Optional[str] = None,
    response_text: Optional[str] = None,
    exception: Optional[BaseException] = None,
    attempts: Optional[int] = None,
    request_id: Optional[str] = None,
) -> str:
    lines = ["<b>Zveno ошибка</b>"]
    lines.append(f"<b>URL:</b> <code>{html.escape(url)}</code>")
    if model:
        lines.append(f"<b>Модель:</b> <code>{html.escape(model)}</code>")
    if status is not None:
        lines.append(f"<b>Статус:</b> <code>{status}</code>")
    if attempts is not None:
        lines.append(f"<b>Попыток:</b> <code>{attempts}</code>")
    if request_id:
        lines.append(f"<b>Request ID:</b> <code>{html.escape(request_id)}</code>")
    if reason:
        lines.append(f"<b>Причина:</b> {html.escape(reason)}")
    if response_text:
        lines.append(f"<pre>{html.escape(_truncate(response_text))}</pre>")
    if exception is not None:
        lines.append(f"<pre>{html.escape(repr(exception))}</pre>")
    return "\n".join(lines)


def _result_error(
    *,
    reason: Optional[str],
    url: str,
    model: Optional[str],
    status: Optional[int] = None,
    response_text: Optional[str] = None,
    exception: Optional[BaseException] = None,
    attempts: Optional[int] = None,
    request_id: Optional[str] = None,
) -> ZvenoResult:
    return ZvenoResult(
        ok=False,
        text=None,
        error_html=_format_error_html(
            url=url,
            model=model,
            status=status,
            reason=reason,
            response_text=response_text,
            exception=exception,
            attempts=attempts,
            request_id=request_id,
        ),
        status=status,
        model=model,
    )

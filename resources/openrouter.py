"""Простой Dagster-ресурс и async-клиент для генерации поздравлений через OpenRouter."""

from __future__ import annotations

import asyncio
import html
import json
from dataclasses import dataclass
from typing import Dict, Iterable, Optional
from urllib.parse import urljoin

import aiohttp
from dagster import Field, InitResourceContext, IntSource, StringSource, resource

from config import openrouter_settings


@dataclass(frozen=True)
class OpenRouterResult:
    """Результат генерации поздравления."""

    ok: bool
    text: Optional[str]
    error_html: Optional[str]
    status: Optional[int]
    model: Optional[str]


class OpenRouterClient:
    """Минимальный async-клиент OpenRouter только для поздравлений сотрудников."""

    def __init__(
        self,
        *,
        api_key: Optional[str],
        base_url: str,
        default_model: str,
        timeout_seconds: int = 60,
        retries: int = 3,
        http_referer: Optional[str] = None,
        app_title: Optional[str] = None,
    ) -> None:
        """Сохраняет настройки клиента."""
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.http_referer = http_referer
        self.app_title = app_title
        self._shared_session: Optional[aiohttp.ClientSession] = None
        self._request_semaphore: Optional[asyncio.Semaphore] = None

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
    ) -> OpenRouterResult:
        """Генерирует готовое поздравление сотруднику."""
        request_model = model or self.default_model
        if not self.api_key:
            return _result_error(
                reason="Не задан OPENROUTER_API_KEY",
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
                {
                    "role": "user",
                    "content": "\n".join(prompt_lines),
                },
            ],
            "temperature": 0.8,
        }

        return await self._request_greeting(payload=payload, model=request_model)

    async def generate_employee_greeting(
        self,
        *,
        employee_name: str,
        occasion: str,
        language: str = "ru",
        age: Optional[int] = None,
        details: Optional[Iterable[str]] = None,
        tone: str = "теплый, уважительный и живой",
        model: Optional[str] = None,
    ) -> OpenRouterResult:
        """Совместимый alias для будущего кода."""
        return await self.generate_greeting(
            employee_name=employee_name,
            occasion=occasion,
            language=language,
            age=age,
            details=details,
            tone=tone,
            model=model,
        )

    def configure_runtime(
        self,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        request_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        """Подключает общий aiohttp session для серии поздравлений в рамках одного job."""
        self._shared_session = session
        self._request_semaphore = request_semaphore

    def reset_runtime(self) -> None:
        """Сбрасывает временные runtime-зависимости после завершения job."""
        self._shared_session = None
        self._request_semaphore = None

    async def _request_greeting(
        self,
        *,
        payload: Dict[str, object],
        model: str,
    ) -> OpenRouterResult:
        """Отправляет запрос в OpenRouter и возвращает готовый текст поздравления."""
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
                    semaphore_context = (
                        self._request_semaphore
                        if self._request_semaphore is not None
                        else _null_async_context()
                    )
                    async with semaphore_context:
                        async with session.post(
                            url,
                            json=payload,
                            headers=_build_headers(
                                api_key=self.api_key,
                                http_referer=self.http_referer,
                                app_title=self.app_title,
                            ),
                        ) as response:
                            status = response.status
                            response_headers = dict(response.headers)
                            response_text = await response.text()
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
                            reason="Ответ OpenRouter не является JSON",
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
                        return OpenRouterResult(
                            ok=True,
                            text=text,
                            error_html=None,
                            status=status,
                            model=_extract_model(data) or model,
                        )

                    return _result_error(
                        reason="OpenRouter не вернул текст поздравления",
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


@resource(
    config_schema={
        "api_key": Field(StringSource, is_required=False),
        "base_url": Field(StringSource, is_required=False),
        "model": Field(StringSource, is_required=False),
        "timeout_seconds": Field(IntSource, is_required=False),
        "retries": Field(IntSource, is_required=False),
        "http_referer": Field(StringSource, is_required=False),
        "app_title": Field(StringSource, is_required=False),
    }
)
def openrouter_resource(context: InitResourceContext) -> OpenRouterClient:
    """Dagster-ресурс OpenRouter с простым клиентом для поздравлений."""
    cfg = openrouter_settings()
    overrides: Dict[str, str | int | None] = context.resource_config or {}

    return OpenRouterClient(
        api_key=overrides.get("api_key", cfg.api_key),
        base_url=str(overrides.get("base_url", cfg.base_url)),
        default_model=str(overrides.get("model", cfg.model)),
        timeout_seconds=int(overrides.get("timeout_seconds", cfg.timeout_seconds)),
        retries=int(overrides.get("retries", cfg.retries)),
        http_referer=overrides.get("http_referer", cfg.http_referer),
        app_title=overrides.get("app_title", cfg.app_title),
    )


def _chat_completions_url(base_url: str) -> str:
    """Собирает URL OpenRouter chat completions."""
    return urljoin(base_url.rstrip("/") + "/", "chat/completions")


def _build_headers(
    *,
    api_key: Optional[str],
    http_referer: Optional[str],
    app_title: Optional[str],
) -> Dict[str, str]:
    """Строит заголовки запроса к OpenRouter."""
    headers = {
        "Authorization": f"Bearer {api_key or ''}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if http_referer:
        headers["HTTP-Referer"] = http_referer
    if app_title:
        headers["X-OpenRouter-Title"] = app_title
    return headers


def _extract_text(data: Dict[str, object]) -> Optional[str]:
    """Достает текст из OpenAI-совместимого ответа OpenRouter."""
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
    return None


def _extract_model(data: Dict[str, object]) -> Optional[str]:
    """Извлекает имя модели из ответа."""
    model = data.get("model")
    return model if isinstance(model, str) and model.strip() else None


def _extract_error_reason(response_text: Optional[str]) -> Optional[str]:
    """Пытается достать человекочитаемую ошибку из ответа OpenRouter."""
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
    """Возвращает задержку перед повторным запросом."""
    if status == 429 and headers:
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
    return min(2 ** (attempt - 1), 8)


def _extract_request_id(headers: Dict[str, str]) -> Optional[str]:
    """Извлекает request id из ответа OpenRouter."""
    for header in ("x-request-id", "request-id"):
        value = headers.get(header)
        if value:
            return value
    return None


def _truncate(text: str, limit: int = 2000) -> str:
    """Обрезает длинный текст для ошибок."""
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)]}…"


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
    """Формирует понятный HTML-текст ошибки для логов или Telegram."""
    lines = ["<b>OpenRouter ошибка</b>"]
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
) -> OpenRouterResult:
    """Строит результат с ошибкой."""
    return OpenRouterResult(
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

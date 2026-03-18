"""Dagster-ресурс и async-клиент для работы с Dodo API."""

from __future__ import annotations

import asyncio
import html
import contextlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
from dagster import Field, InitResourceContext, IntSource, resource

from config import dodo_api_settings
from database.models import Account, Token
from database.requests import load_account_and_token
from resources.database import DatabaseResource


@dataclass(frozen=True)
class DodoApiResult:
    """Результат запроса к Dodo API."""

    ok: bool
    status: Optional[int]
    data: Optional[Any]
    error_html: Optional[str]


class DodoApiClient:
    """Асинхронный клиент для Dodo API с retry по 429/500 и сетевым ошибкам."""

    def __init__(
        self,
        db: DatabaseResource,
        timeout_seconds: int = 30,
        retries_500: int = 3,
        max_concurrent_requests: int = 10,
    ) -> None:
        """Сохраняет ресурсы и runtime-настройки клиента Dodo API."""
        self._db = db
        self.timeout_seconds = timeout_seconds
        self.retries_500 = retries_500
        self.max_concurrent_requests = max_concurrent_requests
        self._account_token_cache: Dict[int, Tuple[Optional[Account], Optional[Token]]] = {}
        self._account_token_cache_lock = asyncio.Lock()
        self._shared_session: Optional[aiohttp.ClientSession] = None
        self._request_semaphore: Optional[asyncio.Semaphore] = None

    async def get(
        self,
        account_id: int,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> DodoApiResult:
        """Выполняет GET-запрос к Dodo API для указанного account_id."""
        try:
            account, token = await self._load_account_and_token(account_id)
        except Exception as exc:  # noqa: BLE001
            return _result_error(
                reason="Ошибка при чтении данных из БД",
                method="GET",
                url=path,
                exception=exc,
            )

        if account is None:
            return _result_error(
                reason=f"Аккаунт не найден: account_id={account_id}",
                method="GET",
                url=path,
            )

        if token is None or not token.access:
            return _result_error(
                reason=f"Токен не найден для account_id={account_id}",
                method="GET",
                url=path,
            )

        base_url = _extract_base_url(account)
        if not base_url:
            return _result_error(
                reason=(
                    "Не удалось определить base_url из accounts.properties "
                    "(ожидается формат io/dodopizza/ru)"
                ),
                method="GET",
                url=path,
            )

        url = _build_url(base_url, path)
        return await self._request(
            url,
            token.access,
            params=params,
            headers=headers,
        )

    async def _load_account_and_token(
        self,
        account_id: int,
    ) -> Tuple[Optional[Account], Optional[Token]]:
        """Загружает и кеширует аккаунт с токеном по account_id."""
        cached = self._account_token_cache.get(account_id)
        if cached is not None:
            return cached

        async with self._account_token_cache_lock:
            cached = self._account_token_cache.get(account_id)
            if cached is not None:
                return cached

            loaded = await load_account_and_token(self._db, account_id)
            self._account_token_cache[account_id] = loaded
            return loaded

    def prime_account_token_cache(
        self,
        data: Dict[int, Tuple[Optional[Account], Optional[Token]]],
    ) -> None:
        """Предзагружает кеш аккаунтов и токенов bulk-данными."""
        self._account_token_cache.update(data)

    def clear_account_token_cache(self) -> None:
        """Очищает кеш аккаунтов и токенов."""
        self._account_token_cache.clear()

    def configure_runtime(
        self,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        request_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        """Подключает общий session и semaphore для runtime job."""
        self._shared_session = session
        self._request_semaphore = request_semaphore

    def reset_runtime(self) -> None:
        """Сбрасывает runtime-зависимости, переданные из job."""
        self._shared_session = None
        self._request_semaphore = None

    async def _request(
        self,
        url: str,
        token: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> DodoApiResult:
        """Выполняет HTTP-запрос с собственным или общим aiohttp session."""
        if self._shared_session is not None:
            return await self._request_with_session(
                self._shared_session,
                url,
                token,
                params=params,
                headers=headers,
            )

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            return await self._request_with_session(
                session,
                url,
                token,
                params=params,
                headers=headers,
            )

    async def _request_with_session(
        self,
        session: aiohttp.ClientSession,
        url: str,
        token: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> DodoApiResult:
        """Делает GET-запрос с retry и преобразует ответ в DodoApiResult."""
        retries_left = self.retries_500
        attempts = 0

        while True:
            attempts += 1
            response_text = None
            response_headers: Dict[str, str] = {}
            status: Optional[int] = None

            try:
                semaphore_context = (
                    self._request_semaphore
                    if self._request_semaphore is not None
                    else _null_async_context()
                )
                async with semaphore_context:
                    async with session.get(
                        url,
                        params=params,
                        headers=_build_headers(token, headers),
                    ) as response:
                        status = response.status
                        response_headers = dict(response.headers)
                        response_text = await response.text()
            except asyncio.TimeoutError as exc:
                if retries_left > 0:
                    retries_left -= 1
                    await asyncio.sleep(_retry_delay(self.retries_500 - retries_left))
                    continue
                return _result_error(
                    reason="Таймаут запроса",
                    method="GET",
                    url=url,
                    exception=exc,
                    attempts=attempts,
                )
            except aiohttp.ClientError as exc:
                if retries_left > 0:
                    retries_left -= 1
                    await asyncio.sleep(_retry_delay(self.retries_500 - retries_left))
                    continue
                return _result_error(
                    reason="Сетевая ошибка",
                    method="GET",
                    url=url,
                    exception=exc,
                    attempts=attempts,
                )
            except Exception as exc:  # noqa: BLE001
                return _result_error(
                    reason="Неожиданная ошибка клиента",
                    method="GET",
                    url=url,
                    exception=exc,
                    attempts=attempts,
                )

            if status in {429, 500} and retries_left > 0:
                retries_left -= 1
                await asyncio.sleep(
                    _retry_delay(
                        self.retries_500 - retries_left,
                        status=status,
                        headers=response_headers,
                    )
                )
                continue

            if status == 200:
                try:
                    data = json.loads(response_text or "")
                except Exception as exc:  # noqa: BLE001
                    return _result_error(
                        reason="Ответ не является JSON",
                        method="GET",
                        url=url,
                        status=status,
                        response_text=response_text,
                        exception=exc,
                        attempts=attempts,
                        request_id=_extract_request_id(response_headers),
                    )

                return DodoApiResult(
                    ok=True,
                    status=status,
                    data=data,
                    error_html=None,
                )

            return _result_error(
                reason="Неуспешный статус ответа",
                method="GET",
                url=url,
                status=status,
                response_text=response_text,
                attempts=attempts,
                request_id=_extract_request_id(response_headers),
            )


@resource(
    config_schema={
        "timeout_seconds": Field(IntSource, is_required=False),
        "retries_500": Field(IntSource, is_required=False),
        "max_concurrent_requests": Field(IntSource, is_required=False),
    },
    required_resource_keys={"db"},
)
def dodo_api_resource(context: InitResourceContext) -> DodoApiClient:
    """Dagster-ресурс Dodo API с override параметрами из config_schema."""
    cfg = dodo_api_settings()
    overrides: Dict[str, Any] = context.resource_config or {}

    timeout_seconds = overrides.get("timeout_seconds", cfg.timeout_seconds)
    retries_500 = overrides.get("retries_500", cfg.retries_500)
    max_concurrent_requests = overrides.get(
        "max_concurrent_requests",
        cfg.max_concurrent_requests,
    )

    return DodoApiClient(
        db=context.resources.db,
        timeout_seconds=timeout_seconds,
        retries_500=retries_500,
        max_concurrent_requests=max_concurrent_requests,
    )


def _build_url(base_url: str, path: str) -> str:
    """Собирает абсолютный URL из base_url и path."""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _build_headers(token: str, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Строит стандартные заголовки запроса к Dodo API."""
    base_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "user-agent": "DodoVkus",
    }
    if headers:
        base_headers.update(headers)
    return base_headers


def _extract_base_url(account: Account) -> Optional[str]:
    """Определяет base_url API по properties или sub аккаунта."""
    properties = (account.properties or "").strip()
    if properties:
        if properties.startswith("http://") or properties.startswith("https://"):
            return properties
        normalized = properties.strip("/")
        if normalized.startswith("api.dodois."):
            return f"https://{normalized}/"
        return f"https://api.dodois.{normalized}/"

    # fallback на случай пустого properties
    if account.sub:
        return _sub_to_url(account.sub)

    return None


def _sub_to_url(sub: str) -> str:
    """Преобразует значение sub в базовый URL API."""
    cleaned = sub.strip()
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    if "." in cleaned:
        return f"https://{cleaned}"
    return f"https://api.dodois.io/{cleaned}"


def _retry_delay(
    attempt: int,
    *,
    status: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
) -> float:
    """Возвращает задержку перед повтором запроса к Dodo API."""
    if status == 429 and headers:
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
    # Небольшая экспоненциальная пауза: 1s, 2s, 4s...
    return min(2 ** (attempt - 1), 8)


def _truncate(text: str, limit: int = 2000) -> str:
    """Обрезает длинный текст для логов и HTML-ошибок."""
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)]}…"


def _extract_request_id(headers: Dict[str, str]) -> Optional[str]:
    """Извлекает request id из известных заголовков ответа."""
    for header in ("x-request-id", "x-correlation-id", "request-id"):
        value = headers.get(header)
        if value:
            return value
    return None


def _format_error_html(
    *,
    method: str,
    url: str,
    status: Optional[int] = None,
    reason: Optional[str] = None,
    response_text: Optional[str] = None,
    exception: Optional[BaseException] = None,
    attempts: Optional[int] = None,
    request_id: Optional[str] = None,
) -> str:
    """Формирует HTML-текст ошибки Dodo API для отправки в чат."""
    lines = ["<b>Dodo API ошибка</b>"]
    lines.append(f"<b>Метод:</b> <code>{html.escape(method)}</code>")
    lines.append(f"<b>URL:</b> <code>{html.escape(url)}</code>")
    if status is not None:
        lines.append(f"<b>Статус:</b> <code>{status}</code>")
    if attempts is not None:
        lines.append(f"<b>Попыток:</b> <code>{attempts}</code>")
    if request_id:
        lines.append(f"<b>Request ID:</b> <code>{html.escape(request_id)}</code>")
    if reason:
        lines.append(f"<b>Причина:</b> {html.escape(reason)}")
    if response_text:
        lines.append("<b>Ответ:</b>")
        lines.append(f"<pre>{html.escape(_truncate(response_text))}</pre>")
    if exception:
        lines.append("<b>Исключение:</b>")
        lines.append(f"<pre>{html.escape(str(exception))}</pre>")
    return "\n".join(lines)


def _result_error(
    *,
    reason: str,
    method: str,
    url: str,
    status: Optional[int] = None,
    response_text: Optional[str] = None,
    exception: Optional[BaseException] = None,
    attempts: Optional[int] = None,
    request_id: Optional[str] = None,
) -> DodoApiResult:
    """Возвращает унифицированный DodoApiResult с текстом ошибки."""
    return DodoApiResult(
        ok=False,
        status=status,
        data=None,
        error_html=_format_error_html(
            method=method,
            url=url,
            status=status,
            reason=reason,
            response_text=response_text,
            exception=exception,
            attempts=attempts,
            request_id=request_id,
        ),
    )


@contextlib.asynccontextmanager
async def _null_async_context():
    """Пустой async context manager для единообразной работы с semaphore."""
    yield

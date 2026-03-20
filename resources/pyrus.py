"""Dagster-ресурс и async-клиент для работы с Pyrus."""

from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin

import aiohttp
from dagster import BoolSource, Field, InitResourceContext, IntSource, StringSource, resource

from config import pyrus_settings


class PyrusClientError(RuntimeError):
    """Базовая ошибка клиента Pyrus."""


@dataclass
class PyrusClient:
    """Небольшой async-клиент Pyrus для job по клиентским тикетам."""

    login: Optional[str]
    security_key: Optional[str]
    base_url: str
    timeout_seconds: int
    retries: int
    verify_ssl: bool
    max_concurrent_requests: int
    tickets_catalog_id: int
    tickets_form_id: int

    def __post_init__(self) -> None:
        self._base_url = self.base_url.rstrip("/") + "/"
        self._shared_session: Optional[aiohttp.ClientSession] = None
        self._request_semaphore: Optional[asyncio.Semaphore] = None
        self._auth_lock = asyncio.Lock()
        self._access_token: Optional[str] = None

    def configure_runtime(
        self,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        request_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        """Подключает общий session/semaphore на время одного job."""
        self._shared_session = session
        self._request_semaphore = request_semaphore

    def reset_runtime(self) -> None:
        """Сбрасывает временные runtime-зависимости после завершения job."""
        self._shared_session = None
        self._request_semaphore = None
        self._access_token = None

    async def get_catalog_items(self, catalog_id: Optional[int] = None) -> list[dict[str, Any]]:
        """Возвращает элементы каталога Pyrus для сопоставления `uuid -> item_id`."""
        target_catalog_id = catalog_id or self.tickets_catalog_id
        data = await self._request_json("GET", f"catalogs/{target_catalog_id}")
        items = data.get("items")
        if not isinstance(items, list):
            raise PyrusClientError("Pyrus catalog response does not contain items list")
        return [item for item in items if isinstance(item, dict)]

    async def search_form_tasks(
        self,
        *,
        created_after: datetime,
        created_before: datetime,
        catalog_item_ids: Iterable[int],
        field_ids: Optional[Iterable[int]] = None,
        form_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Возвращает список задач формы за окно времени в UTC."""
        item_ids = [str(item_id) for item_id in catalog_item_ids if item_id]
        if not item_ids:
            return []

        payload: Dict[str, Any] = {
            "field_ids": list(field_ids or [198, 284, 285, 286, 280, 13]),
            "include_archived": "y",
            "created_after": _format_utc(created_after),
            "created_before": _format_utc(created_before),
            "fld198": ",".join(item_ids),
        }
        target_form_id = form_id or self.tickets_form_id
        data = await self._request_json(
            "POST",
            f"forms/{target_form_id}/register",
            json_body=payload,
        )
        tasks = data.get("tasks")
        if not isinstance(tasks, list):
            return []
        return [task for task in tasks if isinstance(task, dict)]

    async def get_task(self, task_id: int) -> dict[str, Any]:
        """Возвращает полную карточку задачи Pyrus."""
        data = await self._request_json("GET", f"tasks/{task_id}")
        task = data.get("task")
        if not isinstance(task, dict):
            raise PyrusClientError(
                f"Pyrus task payload does not contain task object: task_id={task_id}"
            )
        return task

    async def _get_access_token(self) -> str:
        """Авторизуется в Pyrus и кеширует bearer token на время job."""
        if not self.login or not self.security_key:
            raise PyrusClientError("Не заданы PYRUS_TOKEN/PYRUS_KEY")

        if self._access_token:
            return self._access_token

        async with self._auth_lock:
            if self._access_token:
                return self._access_token

            data = await self._request_json(
                "POST",
                "auth",
                json_body={
                    "login": self.login,
                    "security_key": self.security_key,
                },
                with_auth=False,
            )
            access_token = data.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                raise PyrusClientError("Pyrus auth response does not contain access_token")
            self._access_token = access_token
            return access_token

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        with_auth: bool = True,
    ) -> Dict[str, Any]:
        """Выполняет JSON-запрос к Pyrus с retry и автообновлением bearer token."""
        url = urljoin(self._base_url, path.lstrip("/"))
        attempts = 0
        auth_retry_allowed = with_auth

        session, close_session = self._runtime_session()

        try:
            while True:
                attempts += 1
                token = await self._get_access_token() if with_auth else None
                response_text: Optional[str] = None
                status: Optional[int] = None
                headers: Dict[str, str] = {
                    "Content-Type": "application/json",
                    "user-agent": "AidaFunctions",
                }
                if token:
                    headers["Authorization"] = f"Bearer {token}"

                try:
                    semaphore_context = (
                        self._request_semaphore
                        if self._request_semaphore is not None
                        else _null_async_context()
                    )
                    async with semaphore_context:
                        async with session.request(
                            method,
                            url,
                            json=json_body,
                            headers=headers,
                            ssl=None if self.verify_ssl else False,
                        ) as response:
                            status = response.status
                            response_text = await response.text()
                except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                    if attempts <= self.retries:
                        await asyncio.sleep(_retry_delay(attempts))
                        continue
                    raise PyrusClientError(
                        f"Pyrus request failed: method={method} url={url} attempts={attempts} error={exc}"
                    ) from exc

                if status == 401 and with_auth and auth_retry_allowed:
                    self._access_token = None
                    auth_retry_allowed = False
                    continue

                if status in {429, 500, 502, 503, 504} and attempts <= self.retries:
                    await asyncio.sleep(_retry_delay(attempts))
                    continue

                if status is None or status < 200 or status >= 300:
                    raise PyrusClientError(
                        "Pyrus request returned non-success status: "
                        f"method={method} url={url} status={status} "
                        f"response={_trim_text(response_text)}"
                    )

                try:
                    data = json.loads(response_text or "{}")
                except Exception as exc:  # noqa: BLE001
                    raise PyrusClientError(
                        "Pyrus response is not valid JSON: "
                        f"method={method} url={url} status={status} "
                        f"response={_trim_text(response_text)}"
                    ) from exc

                if not isinstance(data, dict):
                    raise PyrusClientError(
                        f"Pyrus response must be JSON object: method={method} url={url} status={status}"
                    )
                return data
        finally:
            if close_session:
                await session.close()

    def _runtime_session(self) -> tuple[aiohttp.ClientSession, bool]:
        """Возвращает session для одного запроса и признак, нужно ли ее закрывать."""
        if self._shared_session is not None:
            return self._shared_session, False

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        return aiohttp.ClientSession(timeout=timeout, trust_env=True), True


def _trim_text(value: Optional[str], limit: int = 500) -> str:
    """Обрезает длинный ответ API, чтобы лог оставался читаемым."""
    if not value:
        return ""
    return value if len(value) <= limit else f"{value[:limit]}..."


def _retry_delay(attempt: int) -> float:
    """Возвращает задержку перед повтором запроса."""
    return min(0.5 * (2 ** (attempt - 1)), 4.0)


def _format_utc(value: datetime) -> str:
    """Форматирует datetime в UTC-строку, которую ожидает Pyrus API."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextlib.asynccontextmanager
async def _null_async_context():
    """Пустой async-контекст, когда внешний semaphore не передан."""
    yield


@resource(
    config_schema={
        "login": Field(StringSource, is_required=False),
        "security_key": Field(StringSource, is_required=False),
        "base_url": Field(StringSource, is_required=False),
        "timeout_seconds": Field(IntSource, is_required=False),
        "retries": Field(IntSource, is_required=False),
        "verify_ssl": Field(BoolSource, is_required=False),
        "max_concurrent_requests": Field(IntSource, is_required=False),
        "tickets_catalog_id": Field(IntSource, is_required=False),
        "tickets_form_id": Field(IntSource, is_required=False),
    }
)
def pyrus_resource(context: InitResourceContext) -> PyrusClient:
    """Dagster-ресурс для Pyrus с настройками из env и override через config_schema."""
    cfg = pyrus_settings()
    overrides: Dict[str, Any] = context.resource_config or {}
    return PyrusClient(
        login=overrides.get("login", cfg.login),
        security_key=overrides.get("security_key", cfg.security_key),
        base_url=overrides.get("base_url", cfg.base_url),
        timeout_seconds=overrides.get("timeout_seconds", cfg.timeout_seconds),
        retries=overrides.get("retries", cfg.retries),
        verify_ssl=overrides.get("verify_ssl", cfg.verify_ssl),
        max_concurrent_requests=overrides.get(
            "max_concurrent_requests", cfg.max_concurrent_requests
        ),
        tickets_catalog_id=overrides.get(
            "tickets_catalog_id", cfg.tickets_catalog_id
        ),
        tickets_form_id=overrides.get("tickets_form_id", cfg.tickets_form_id),
    )

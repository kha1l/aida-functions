"""Утилиты для формирования и публикации сообщений в RabbitMQ."""

import asyncio
import contextlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from config import rabbitmq_settings


REQUIRED_PAYLOAD_KEYS = ("chat_id", "post", "text")
OPTIONAL_PAYLOAD_KEYS = ("topic_id",)
PUBLISH_RETRY_ATTEMPTS = 3


def build_payload(
    *,
    chat_id: Any,
    post: Any,
    text: str,
    topic_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Собирает payload для очереди и валидирует его."""
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "post": post,
        "text": text,
    }
    if topic_id is not None:
        payload["topic_id"] = topic_id
    validate_payload(payload)
    return payload


def validate_payload(payload: Dict[str, Any]) -> None:
    """Проверяет структуру payload и пригодность к JSON."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    for key in REQUIRED_PAYLOAD_KEYS:
        if key not in payload:
            raise KeyError(f"payload missing required key: {key}")

    text = payload.get("text")
    if not isinstance(text, str) or not text:
        raise ValueError("payload.text must be a non-empty string")

    chat_id = payload.get("chat_id")
    if not isinstance(chat_id, (int, str)):
        raise ValueError("payload.chat_id must be int or str")

    topic_id = payload.get("topic_id")
    if topic_id is not None and not isinstance(topic_id, int):
        raise ValueError("payload.topic_id must be int when provided")

    try:
        json.dumps(payload)
    except TypeError as exc:
        raise ValueError("payload must be JSON-serializable") from exc


@dataclass
class RabbitMQPublisher:
    """Публикатор сообщений в RabbitMQ (sync/async)."""

    url: str
    queue: str
    exchange: str
    routing_key: str
    exchange_type: str
    durable: bool
    enabled: bool = True
    _connection: Any = field(default=None, init=False, repr=False)
    _channel: Any = field(default=None, init=False, repr=False)
    _exchange_obj: Any = field(default=None, init=False, repr=False)
    _transport_lock: Any = field(default=None, init=False, repr=False)

    @classmethod
    def from_env(cls) -> "RabbitMQPublisher":
        """Создает публикатор из env-конфига."""
        cfg = rabbitmq_settings()
        return cls(
            url=cfg.url,
            queue=cfg.queue,
            exchange=cfg.exchange,
            routing_key=cfg.routing_key,
            exchange_type=cfg.exchange_type,
            durable=cfg.durable,
            enabled=cfg.enabled,
        )

    def publish(self, payload: Dict[str, Any]) -> None:
        """Синхронная публикация (не для активного event loop)."""
        validate_payload(payload)
        if not self.enabled:
            return
        try:
            import asyncio

            asyncio.run(self.publish_async(payload))
        except RuntimeError as exc:
            raise RuntimeError(
                "RabbitMQPublisher.publish cannot run inside an event loop; "
                "use publish_async instead."
            ) from exc

    async def publish_async(self, payload: Dict[str, Any]) -> None:
        """Асинхронная публикация в RabbitMQ."""
        validate_payload(payload)
        if not self.enabled:
            return

        for attempt in range(1, PUBLISH_RETRY_ATTEMPTS + 1):
            try:
                await self._publish_with_open_transport(payload)
                return
            except Exception:
                if attempt >= PUBLISH_RETRY_ATTEMPTS:
                    raise
                await self._reset_transport()
                await asyncio.sleep(min(2 ** (attempt - 1), 4))

    async def open_async(self) -> None:
        """Подготавливает и кэширует соединение/канал с RabbitMQ."""
        if not self.enabled:
            return
        await self._ensure_transport()

    async def close_async(self) -> None:
        """Закрывает кэшированное соединение с RabbitMQ."""
        await self._reset_transport()

    async def _publish_with_open_transport(self, payload: Dict[str, Any]) -> None:
        """Публикует payload в уже подготовленный transport."""
        aio_pika = _import_aio_pika()
        await self._ensure_transport()

        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            if self.durable
            else aio_pika.DeliveryMode.NOT_PERSISTENT,
        )

        if self.exchange:
            routing_key = self.routing_key or self.queue
            await self._exchange_obj.publish(message, routing_key=routing_key)
        else:
            await self._channel.default_exchange.publish(
                message,
                routing_key=self.queue,
            )

    async def _ensure_transport(self) -> None:
        """Поднимает и кеширует соединение, канал и exchange при необходимости."""
        aio_pika = _import_aio_pika()
        lock = self._get_transport_lock()

        async with lock:
            if (
                self._connection is not None
                and not self._connection.is_closed
                and self._channel is not None
                and not self._channel.is_closed
            ):
                return

            self._connection = await aio_pika.connect_robust(self.url)
            self._channel = await self._connection.channel()

            if self.queue:
                await self._channel.declare_queue(self.queue, durable=self.durable)

            if self.exchange:
                self._exchange_obj = await self._channel.declare_exchange(
                    self.exchange,
                    type=self.exchange_type,
                    durable=self.durable,
                )
            else:
                self._exchange_obj = None

    async def _reset_transport(self) -> None:
        """Сбрасывает кешированный transport и закрывает соединение."""
        lock = self._get_transport_lock()
        async with lock:
            if self._connection is not None:
                with contextlib.suppress(Exception):
                    await self._connection.close()
            self._connection = None
            self._channel = None
            self._exchange_obj = None

    def _get_transport_lock(self):
        """Ленивая инициализация lock для операций с transport."""
        if self._transport_lock is None:
            self._transport_lock = asyncio.Lock()
        return self._transport_lock


def _import_aio_pika():
    """Импортирует aio-pika с понятной ошибкой при отсутствии зависимости."""
    try:
        import aio_pika
    except ImportError as exc:
        raise ImportError(
            "aio-pika is required for RabbitMQ publishing. "
            "Add it to requirements.txt"
        ) from exc
    return aio_pika

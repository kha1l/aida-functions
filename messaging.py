"""Утилиты для формирования и публикации сообщений в RabbitMQ."""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from config import rabbitmq_settings


REQUIRED_PAYLOAD_KEYS = ("chat_id", "post", "text")
OPTIONAL_PAYLOAD_KEYS = ("topic_id",)


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


@dataclass(frozen=True)
class RabbitMQPublisher:
    """Публикатор сообщений в RabbitMQ (sync/async)."""

    url: str
    queue: str
    exchange: str
    routing_key: str
    exchange_type: str
    durable: bool
    enabled: bool = True

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

        try:
            import aio_pika
        except ImportError as exc:
            raise ImportError(
                "aio-pika is required for RabbitMQ publishing. "
                "Add it to requirements.txt"
            ) from exc

        connection = await aio_pika.connect_robust(self.url)
        channel = await connection.channel()

        if self.queue:
            await channel.declare_queue(self.queue, durable=self.durable)

        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            if self.durable
            else aio_pika.DeliveryMode.NOT_PERSISTENT,
        )

        if self.exchange:
            exchange = await channel.declare_exchange(
                self.exchange,
                type=self.exchange_type,
                durable=self.durable,
            )
            routing_key = self.routing_key or self.queue
            await exchange.publish(message, routing_key=routing_key)
        else:
            await channel.default_exchange.publish(
                message,
                routing_key=self.queue,
            )

        await connection.close()

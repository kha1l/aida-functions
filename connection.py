"""Небольшие фабрики для локального создания подключений и клиентов."""

from config import database_settings, pyrus_settings
from database.engine import build_engine
from messaging import RabbitMQPublisher
from resources.pyrus import PyrusClient


def create_db_engine():
    """Создаёт SQLAlchemy engine из текущих env-настроек."""
    cfg = database_settings()
    return build_engine(cfg.url, echo=cfg.echo, pool_pre_ping=cfg.pool_pre_ping)


def create_rabbitmq_publisher() -> RabbitMQPublisher:
    """Создаёт RabbitMQ-публикатор из env-конфига."""
    return RabbitMQPublisher.from_env()


def create_pyrus_client() -> PyrusClient:
    """Создаёт Pyrus-клиент из env-конфига."""
    cfg = pyrus_settings()
    return PyrusClient(
        login=cfg.login,
        security_key=cfg.security_key,
        base_url=cfg.base_url,
        timeout_seconds=cfg.timeout_seconds,
        retries=cfg.retries,
        verify_ssl=cfg.verify_ssl,
        max_concurrent_requests=cfg.max_concurrent_requests,
        tickets_catalog_id=cfg.tickets_catalog_id,
        tickets_form_id=cfg.tickets_form_id,
    )

from config import database_settings, openrouter_settings
from database.engine import build_engine
from messaging import RabbitMQPublisher
from resources.openrouter import OpenRouterClient


def create_db_engine():
    """Создаёт SQLAlchemy engine из текущих env-настроек."""
    cfg = database_settings()
    return build_engine(cfg.url, echo=cfg.echo, pool_pre_ping=cfg.pool_pre_ping)


def create_rabbitmq_publisher() -> RabbitMQPublisher:
    """Создаёт RabbitMQ-публикатор из env-конфига."""
    return RabbitMQPublisher.from_env()


def create_openrouter_client() -> OpenRouterClient:
    """Создаёт OpenRouter-клиент из env-конфига."""
    cfg = openrouter_settings()
    return OpenRouterClient(
        api_key=cfg.api_key,
        base_url=cfg.base_url,
        default_model=cfg.model,
        timeout_seconds=cfg.timeout_seconds,
        retries=cfg.retries,
        http_referer=cfg.http_referer,
        app_title=cfg.app_title,
    )

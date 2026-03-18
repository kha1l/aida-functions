from config import database_settings
from database.engine import build_engine
from messaging import RabbitMQPublisher


def create_db_engine():
    """Создаёт SQLAlchemy engine из текущих env-настроек."""
    cfg = database_settings()
    return build_engine(cfg.url, echo=cfg.echo, pool_pre_ping=cfg.pool_pre_ping)


def create_rabbitmq_publisher() -> RabbitMQPublisher:
    """Создаёт RabbitMQ-публикатор из env-конфига."""
    return RabbitMQPublisher.from_env()

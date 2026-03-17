"""Загрузка конфигурации из переменных окружения."""

import os
from dataclasses import dataclass
from typing import Optional


# -------------------
# Вспомогательные функции env
# -------------------

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Возвращает строковое значение env или default."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    """Читает булево значение из env (1/true/yes/y/on)."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    """Читает целое число из env."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


# -------------------
# Расписания
# -------------------

@dataclass(frozen=True)
class ScheduleConfig:
    """Конфигурация расписания для job."""

    name: str
    cron: str
    timezone: str
    enabled: bool


def schedule_config(
    name: str,
    default_cron: str = "0 9 * * *",
    default_timezone: Optional[str] = None,
    default_enabled: bool = True,
) -> ScheduleConfig:
    """Собирает конфигурацию расписания по имени job."""
    if default_timezone is None:
        default_timezone = _env("SCHEDULE_TZ_DEFAULT", "UTC")

    key = name.upper()
    return ScheduleConfig(
        name=name,
        cron=_env(f"SCHEDULE_{key}_CRON", default_cron) or default_cron,
        timezone=_env(f"SCHEDULE_{key}_TZ", default_timezone) or default_timezone,
        enabled=_env_bool(f"SCHEDULE_{key}_ENABLED", default_enabled),
    )


# -------------------
# RabbitMQ
# -------------------

@dataclass(frozen=True)
class RabbitMQSettings:
    """Настройки RabbitMQ."""

    url: str
    queue: str
    exchange: str
    routing_key: str
    exchange_type: str
    durable: bool
    enabled: bool


def rabbitmq_settings() -> RabbitMQSettings:
    """Собирает настройки RabbitMQ из env."""
    return RabbitMQSettings(
        url=_env("RABBIT_URL", "amqp://guest:guest@localhost:5672/"),
        queue=_env("RABBIT_QUEUE", "aida"),
        exchange=_env("RABBIT_EXCHANGE", ""),
        routing_key=_env("RABBIT_ROUTING_KEY", ""),
        exchange_type=_env("RABBIT_EXCHANGE_TYPE", "direct"),
        durable=_env_bool("RABBIT_DURABLE", True),
        enabled=_env_bool("RABBIT_ENABLED", True),
    )


# -------------------
# База данных
# -------------------

@dataclass(frozen=True)
class DatabaseSettings:
    """Настройки подключения к базе данных."""

    url: str
    echo: bool
    pool_pre_ping: bool


def _build_database_url_from_parts() -> str:
    """Собирает DATABASE_URL из частей DBASE_* (без SQLite)."""
    driver = _env("DBASE_DRIVER", "postgresql")
    host = _env("DBASE_HOST")
    name = _env("DBASE_NAME")
    user = _env("DBASE_USER")
    password = _env("DBASE_PASSWORD")
    port = _env("DBASE_PORT")

    missing = [
        key
        for key, value in {
            "DBASE_HOST": host,
            "DBASE_NAME": name,
            "DBASE_USER": user,
            "DBASE_PASSWORD": password,
        }.items()
        if not value
    ]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing database settings: {missing_str}")

    port_part = f":{port}" if port else ""
    return f"{driver}://{user}:{password}@{host}{port_part}/{name}"


def database_settings() -> DatabaseSettings:
    """Возвращает настройки БД, предпочтительно DATABASE_URL."""
    url = _env("DATABASE_URL")
    if not url:
        url = _build_database_url_from_parts()

    return DatabaseSettings(
        url=url,
        echo=_env_bool("DATABASE_ECHO", False),
        pool_pre_ping=_env_bool("DATABASE_POOL_PRE_PING", True),
    )

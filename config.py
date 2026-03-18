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


# -------------------
# Dodo API
# -------------------

@dataclass(frozen=True)
class DodoApiSettings:
    """Настройки доступа к Dodo API."""

    timeout_seconds: int
    retries_500: int
    max_concurrent_requests: int


def dodo_api_settings() -> DodoApiSettings:
    """Собирает настройки Dodo API из env."""
    return DodoApiSettings(
        timeout_seconds=_env_int("DODO_API_TIMEOUT_SECONDS", 30),
        retries_500=_env_int("DODO_API_RETRIES_500", 3),
        max_concurrent_requests=_env_int("DODO_API_MAX_CONCURRENT_REQUESTS", 10),
    )


# -------------------
# Couriers
# -------------------

@dataclass(frozen=True)
class CouriersSettings:
    """Настройки job couriers."""

    max_concurrent_units: int


def couriers_settings() -> CouriersSettings:
    """Собирает настройки couriers job из env."""
    return CouriersSettings(
        max_concurrent_units=_env_int("COURIERS_MAX_CONCURRENT_UNITS", 15),
    )


# -------------------
# Weather API
# -------------------

@dataclass(frozen=True)
class WeatherSettings:
    """Настройки доступа к погодному API."""

    api_key: Optional[str]
    timeout_seconds: int
    forecast_count: int
    lang: str
    retries: int
    max_concurrent_requests: int


def weather_settings() -> WeatherSettings:
    """Собирает настройки OpenWeather из env."""
    return WeatherSettings(
        api_key=_env("WEATHER_API_KEY") or _env("API_WEATHER_KEY"),
        timeout_seconds=_env_int("WEATHER_API_TIMEOUT_SECONDS", 15),
        forecast_count=_env_int("WEATHER_FORECAST_COUNT", 6),
        lang=_env("WEATHER_LANG", "ru") or "ru",
        retries=_env_int("WEATHER_API_RETRIES", 3),
        max_concurrent_requests=_env_int("WEATHER_MAX_CONCURRENT_REQUESTS", 5),
    )

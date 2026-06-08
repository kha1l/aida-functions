"""Загрузка конфигурации из переменных окружения."""

import os
from dataclasses import dataclass
from typing import Iterable, Optional


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


def _env_first(names: Iterable[str], default: Optional[str] = None) -> Optional[str]:
    """Возвращает первое непустое значение из списка env-переменных."""
    for name in names:
        value = _env(name)
        if value is not None:
            return value
    return default


def _env_int_first(names: Iterable[str], default: int) -> int:
    """Читает первое непустое целое значение из списка env-переменных."""
    for name in names:
        value = _env(name)
        if value is not None:
            return int(value)
    return default


def _env_int_list(name: str, default: Iterable[int]) -> tuple[int, ...]:
    """Читает список целых чисел из env в формате `1,2,3`."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return tuple(default)

    result = []
    for part in value.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        hour = int(cleaned)
        if hour < 0 or hour > 23:
            raise ValueError(f"{name} contains invalid hour: {hour}")
        result.append(hour)
    return tuple(result)


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


def schedule_local_hours(name: str, default_hours: Iterable[int]) -> tuple[int, ...]:
    """Возвращает локальные часы запуска для job, которые фильтруют точки внутри op."""
    key = name.upper()
    return _env_int_list(f"SCHEDULE_{key}_HOURS", default_hours)


# -------------------
# RabbitMQ
# -------------------

@dataclass(frozen=True)
class RabbitMQSettings:
    """Настройки RabbitMQ."""

    url: str
    queue: str
    max_queue: str
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
        max_queue=_env("RABBIT_QUEUE_MAX", "aida-max"),
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
# Ratings API
# -------------------

@dataclass(frozen=True)
class RatingsApiSettings:
    """Настройки эндпоинтов рейтингов control API."""

    standards_url: str
    customer_experience_url: str
    recent_hours: int


def ratings_api_settings() -> RatingsApiSettings:
    """Собирает настройки control-рейтингов из env."""
    return RatingsApiSettings(
        standards_url=(
            _env("CONTROL_STANDARD_RATINGS_URL", "https://api.dodois.io/controlling/ratings/standards")
            or "https://api.dodois.io/controlling/ratings/standards"
        ),
        customer_experience_url=(
            _env(
                "CONTROL_CUSTOMER_EXPERIENCE_RATINGS_URL",
                "https://api.dodois.io/controlling/ratings/customer-experience",
            )
            or "https://api.dodois.io/controlling/ratings/customer-experience"
        ),
        recent_hours=_env_int("RATINGS_RECENT_HOURS", 24),
    )


# -------------------
# Store API widget
# -------------------

@dataclass(frozen=True)
class StoreWidgetSettings:
    """Настройки выгрузки данных в Dodo Store API для старого widget."""

    token_url: str
    storage_url: str
    client_id: Optional[str]
    client_secret: Optional[str]
    scope: str
    model_id: Optional[str]
    timeout_seconds: int
    retries: int
    upload_chunk_size: int


def store_widget_settings() -> StoreWidgetSettings:
    """Собирает настройки выгрузки widget-данных из env.

    Поддерживаем как новые явные ключи `STORE_WIDGET_*`, так и старые
    имена из legacy-проекта `widget` (`CLIENT`, `SECRET`, `SCOPE`, `MODEL`).
    """
    return StoreWidgetSettings(
        token_url=_env("STORE_WIDGET_TOKEN_URL", "https://auth.dodois.io/connect/token")
        or "https://auth.dodois.io/connect/token",
        storage_url=_env("STORE_WIDGET_STORAGE_URL", "https://api.dodois.io/store-api/storage")
        or "https://api.dodois.io/store-api/storage",
        client_id=_env("STORE_WIDGET_CLIENT_ID") or _env("CLIENT"),
        client_secret=_env("STORE_WIDGET_CLIENT_SECRET") or _env("SECRET"),
        scope=_env("STORE_WIDGET_SCOPE", _env("SCOPE", "storeapi:write")) or "storeapi:write",
        model_id=_env("STORE_WIDGET_MODEL_ID") or _env("MODEL"),
        timeout_seconds=_env_int("STORE_WIDGET_TIMEOUT_SECONDS", 30),
        retries=_env_int("STORE_WIDGET_RETRIES", 5),
        upload_chunk_size=_env_int("STORE_WIDGET_UPLOAD_CHUNK_SIZE", 100),
    )


@dataclass(frozen=True)
class ZvenoSettings:
    """Настройки доступа к Zveno AI."""

    api_key: Optional[str]
    base_url: str
    model: str
    timeout_seconds: int
    retries: int
    max_concurrent_requests: int
    min_request_interval_seconds: int


def zveno_settings() -> ZvenoSettings:
    """Собирает настройки Zveno из env с такими же алиасами, как в reminder."""
    return ZvenoSettings(
        api_key=_env_first(
            (
                "ZVENOAI_API_KEY",
                "ZVENO_API_KEY",
                "CEREBRAS_API_KEY",
                "TOKEN_AI",
            )
        ),
        base_url=_env_first(
            (
                "ZVENOAI_BASE_URL",
                "ZVENO_BASE_URL",
                "CEREBRAS_BASE_URL",
            ),
            "https://api.zveno.ai/v1",
        )
        or "https://api.zveno.ai/v1",
        model=_env_first(
            (
                "ZVENOAI_MODEL",
                "ZVENO_MODEL",
                "CEREBRAS_MODEL",
            ),
            "qwen/qwen3-8b",
        )
        or "qwen/qwen3-8b",
        timeout_seconds=_env_int_first(
            (
                "ZVENOAI_TIMEOUT_SECONDS",
                "ZVENO_TIMEOUT_SECONDS",
                "CEREBRAS_TIMEOUT_SECONDS",
            ),
            120,
        ),
        retries=_env_int_first(("ZVENOAI_RETRIES", "ZVENO_RETRIES"), 3),
        max_concurrent_requests=_env_int_first(
            (
                "ZVENOAI_MAX_CONCURRENT_REQUESTS",
                "ZVENO_MAX_CONCURRENT_REQUESTS",
            ),
            2,
        ),
        min_request_interval_seconds=_env_int_first(
            (
                "ZVENOAI_MIN_REQUEST_INTERVAL_SECONDS",
                "ZVENO_MIN_REQUEST_INTERVAL_SECONDS",
            ),
            0,
        ),
    )


# -------------------
# Pyrus
# -------------------

@dataclass(frozen=True)
class PyrusSettings:
    """Настройки доступа к Pyrus."""

    login: Optional[str]
    security_key: Optional[str]
    base_url: str
    timeout_seconds: int
    retries: int
    verify_ssl: bool
    max_concurrent_requests: int
    tickets_catalog_id: int
    tickets_form_id: int


def pyrus_settings() -> PyrusSettings:
    """Собирает настройки Pyrus из env."""
    return PyrusSettings(
        login=_env('PYRUS_TOKEN'),
        security_key=_env('PYRUS_KEY'),
        base_url=_env('PYRUS_BASE_URL', 'https://api.pyrus.com/v4') or 'https://api.pyrus.com/v4',
        timeout_seconds=_env_int('PYRUS_TIMEOUT_SECONDS', 30),
        retries=_env_int('PYRUS_RETRIES', 3),
        verify_ssl=_env_bool('PYRUS_VERIFY_SSL', False),
        max_concurrent_requests=_env_int('PYRUS_MAX_CONCURRENT_REQUESTS', 10),
        tickets_catalog_id=_env_int('PYRUS_TICKETS_CATALOG_ID', 56873),
        tickets_form_id=_env_int('PYRUS_TICKETS_FORM_ID', 522023),
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

"""Dagster-ресурс для работы с БД."""

from functools import cached_property
from dataclasses import dataclass
from typing import Any, Dict

from dagster import BoolSource, Field, InitResourceContext, StringSource, resource

from config import database_settings
from database.engine import build_engine, build_sessionmaker


@dataclass(frozen=True)
class DatabaseResource:
    """Обертка над настройками БД и фабриками подключений."""

    url: str
    echo: bool
    pool_pre_ping: bool

    @cached_property
    def engine(self):
        """Создает и кэширует SQLAlchemy engine с текущими настройками."""
        return build_engine(self.url, echo=self.echo, pool_pre_ping=self.pool_pre_ping)

    @cached_property
    def sessionmaker(self):
        """Создает и кэширует sessionmaker поверх engine."""
        return build_sessionmaker(self.engine)

    def get_engine(self):
        """Создает SQLAlchemy engine с текущими настройками."""
        return self.engine

    def get_sessionmaker(self):
        """Создает sessionmaker поверх нового engine."""
        return self.sessionmaker


@resource(
    config_schema={
        "url": Field(StringSource, is_required=False),
        "echo": Field(BoolSource, is_required=False),
        "pool_pre_ping": Field(BoolSource, is_required=False),
    }
)
def db_resource(context: InitResourceContext) -> DatabaseResource:
    """Dagster-ресурс БД с возможностью override через config_schema."""
    cfg = database_settings()
    overrides: Dict[str, Any] = context.resource_config or {}

    return DatabaseResource(
        url=overrides.get("url", cfg.url),
        echo=overrides.get("echo", cfg.echo),
        pool_pre_ping=overrides.get("pool_pre_ping", cfg.pool_pre_ping),
    )

"""Dagster-ресурс для работы с БД."""

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

    def get_engine(self):
        """Создает SQLAlchemy engine с текущими настройками."""
        return build_engine(self.url, echo=self.echo, pool_pre_ping=self.pool_pre_ping)

    def get_sessionmaker(self):
        """Создает sessionmaker поверх нового engine."""
        engine = self.get_engine()
        return build_sessionmaker(engine)


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

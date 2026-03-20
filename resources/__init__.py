"""Экспорт shared Dagster-ресурсов проекта."""

from .database import db_resource
from .dodo_api import dodo_api_resource
from .openrouter import openrouter_resource
from .rabbitmq import rabbitmq_resource
from .pyrus import pyrus_resource

__all__ = [
    "db_resource",
    "dodo_api_resource",
    "openrouter_resource",
    "rabbitmq_resource",
    "pyrus_resource",
]

"""Экспорт shared Dagster-ресурсов проекта."""

from .database import db_resource
from .dodo_api import dodo_api_resource
from .rabbitmq import rabbitmq_resource
from .pyrus import pyrus_resource
from .zveno import zveno_resource

__all__ = [
    "db_resource",
    "dodo_api_resource",
    "rabbitmq_resource",
    "pyrus_resource",
    "zveno_resource",
]

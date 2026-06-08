"""Общие resource_defs для модульных Definitions и корневого defs.py."""

from resources import (
    db_resource,
    dodo_api_resource,
    rabbitmq_resource,
    pyrus_resource,
    zveno_resource,
)


# Один словарь на весь проект: так модульные definitions и корневой defs.py
# используют одинаковый набор shared-ресурсов и не расходятся со временем.
COMMON_RESOURCE_DEFS = {
    "db": db_resource,
    "dodo_api": dodo_api_resource,
    "rabbitmq": rabbitmq_resource,
    "pyrus": pyrus_resource,
    "zveno": zveno_resource,
}

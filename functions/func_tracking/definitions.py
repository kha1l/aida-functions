"""Модульные Definitions для manual job по трекингу."""

from dagster import Definitions

from functions.shared.definition_resources import COMMON_RESOURCE_DEFS

from .jobs import tracking_job


defs = Definitions(
    jobs=[tracking_job],
    resources=COMMON_RESOURCE_DEFS,
)

definitions = defs

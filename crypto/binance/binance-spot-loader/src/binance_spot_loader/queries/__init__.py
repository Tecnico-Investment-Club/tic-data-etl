"""Queries implementation."""

from .base import BaseQueries, BaseQueriesLatest
from .spot import Queries as SpotQueries
from .spot_latest import Queries as SpotLatestQueries


__all__ = [
    "BaseQueries",
    "BaseQueriesLatest",
    "SpotQueries",
    "SpotLatestQueries",
]

"""Contains an enum for the cache functions."""

from enum import Enum


class CacheFunctions(Enum):
    FIND_ONE = 1
    FIND = 2
    AGGREGATE = 3


DEFAULT_CACHE_FUNCTIONS = [
    CacheFunctions.FIND_ONE,
    CacheFunctions.FIND,
    CacheFunctions.AGGREGATE,
]

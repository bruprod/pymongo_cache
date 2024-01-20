"""Contains an enum for the default caching behavior of a collection."""

from enum import Enum


class DefaultCachingBehavior(Enum):
    """Enum for the default caching behavior of a collection."""

    CACHE_ALL = 1
    CACHE_NONE = 2

""" Enumerations for the backends """
import enum
from typing import Type

from cache_backend.CacheBackendBase import CacheBackendBase


class CacheBackend(enum.Enum):
    """Cache backend enumeration"""

    IN_MEMORY = 1
    MONGODB = 2
    SQLITE = 3


class CacheBackendFactory:
    """Cache backend factory"""

    @staticmethod
    def get_cache_backend(cache_backend: CacheBackend) -> Type[CacheBackendBase]:
        """Get the cache backend"""
        if cache_backend == CacheBackend.IN_MEMORY:
            from cache_backend.in_memory_backend.InMemoryCacheBackend import (
                InMemoryCacheBackend,
            )

            return InMemoryCacheBackend
        elif cache_backend == CacheBackend.MONGODB:
            from cache_backend.mongodb_backend.MongoDBCacheBackend import (
                MongoDBCacheBackend,
            )

            return MongoDBCacheBackend
        elif cache_backend == CacheBackend.SQLITE:
            raise NotImplementedError("SQLite cache backend not implemented yet")
        else:
            raise Exception("Invalid cache backend")

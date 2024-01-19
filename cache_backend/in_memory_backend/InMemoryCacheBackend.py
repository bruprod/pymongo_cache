"""Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""
from datetime import datetime
from typing import Dict, Any

from pymongo.collection import Collection

from cache_backend.base.CacheBackendBase import CacheBackendBase
from cache_backend.base.CacheCleanupHandlerBase import CleanupStrategy
from cache_backend.CacheEntry import CacheEntry
from cache_backend.QueryInfo import QueryInfo
from cache_backend.in_memory_backend.InMemoryCacheCleanupHandler import (
    InMemoryCacheCleanupHandler,
)


class InMemoryCacheBackend(CacheBackendBase):
    """Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""

    _cache: Dict[QueryInfo, CacheEntry] = {}

    def __init__(
        self,
        collection: Collection,
        ttl: int = 0,
        max_item_size: int = 1 * 10**6,
        max_num_items: int = 1000,
        cache_cleanup_cycle_time: float = 1,
    ):
        super().__init__(
            collection,
            ttl,
            max_item_size,
            max_num_items,
            cache_cleanup_cycle_time=cache_cleanup_cycle_time,
        )
        self._cache = {}
        self._cache_cleanup_handler = InMemoryCacheCleanupHandler(
            collection,
            max_item_size,
            max_num_items,
            cleanup_strategy=CleanupStrategy.LRU,
        )

    def get(self, key: QueryInfo) -> Any:
        """Get the value from the cache."""
        entry = self._cache.get(key, None)

        if entry is not None:
            # Update the timestamp and access count when the entry is accessed
            entry.timestamp = datetime.now()
            entry.access_count += 1
            return entry.value
        return None

    def set(
        self, key: QueryInfo, value: Any, execution_time_millis, ttl: int = None
    ) -> None:
        """Set the value in the cache.
        :param ttl: The time to live for the key.
        :param value: The value to set.
        :param key: The key to set.
        :param execution_time_millis: The execution time of the query in milliseconds.
        """
        self._cache[key] = CacheEntry(
            key, value, self.collection.name, key.__hash__(), execution_time_millis
        )

    def delete(self, key: QueryInfo) -> None:
        """Delete the value from the cache."""
        if key in self._cache:
            del self._cache[key]

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()

    def get_all(self) -> Dict[QueryInfo, Any]:
        """Get all the values from the cache."""
        return self._cache

    def _cache_cleanup_internal(self) -> None:
        """Clean up the cache."""
        self._cache_cleanup_handler.cleanup_cache()

"""Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""

from typing import Dict, Any

from pymongo.collection import Collection

from cache_backend.CacheBackendBase import CacheBackendBase
from cache_backend.CacheEntry import CacheEntry
from cache_backend.QueryInfo import QueryInfo


class InMemoryCacheBackend(CacheBackendBase):
    """Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""
    _cache: Dict[QueryInfo, CacheEntry] = {}

    def __init__(self, collection: Collection, ttl: int = 0, max_item_size: int = 0):
        super().__init__(collection, ttl, max_item_size)
        self._cache = {}

    def get(self, key: QueryInfo) -> Any:
        """Get the value from the cache."""
        entry = self._cache.get(key, None)

        if entry is not None:
            return entry.value
        return None

    def set(self, key: QueryInfo, value: Any, ttl: int = None) -> None:
        """Set the value in the cache."""
        self._cache[key] = CacheEntry(key, value, self.collection.name, key.__hash__())

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

    def has_item(self, key: QueryInfo) -> bool:
        """Check if the cache has the item."""
        return key in self._cache

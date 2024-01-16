"""Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""

from typing import Dict, Any

from pymongo.collection import Collection

from cache_backend.CacheBackendBase import CacheBackendBase
from cache_backend.QueryInfo import QueryInfo


class MemoryCacheBackend(CacheBackendBase):
    """Implementation of the MemoryCacheBackend class, which implements the CacheBackend interface."""
    _cache: Dict[QueryInfo, Any] = {}

    def __init__(self, collection: Collection):
        super().__init__(collection)
        self._cache = {}

    def get(self, key: QueryInfo) -> Any:
        """Get the value from the cache."""
        return self._cache.get(key, None)

    def set(self, key: QueryInfo, value: Any, ttl: int = None) -> None:
        """Set the value in the cache."""
        self._cache[key] = value

    def delete(self, key: QueryInfo) -> None:
        """Delete the value from the cache."""
        if key in self._cache:
            del self._cache[key]

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()

    def get_ttl(self, key: QueryInfo) -> int:
        """Get the TTL for the key."""
        return self._ttl

    def set_ttl(self, key: QueryInfo, ttl: int) -> None:
        """Set the TTL for the key."""
        self._ttl = ttl

    def get_all(self) -> Dict[QueryInfo, Any]:
        """Get all the values from the cache."""
        return self._cache

    def has_item(self, key: QueryInfo) -> bool:
        """Check if the cache has the item."""
        return key in self._cache

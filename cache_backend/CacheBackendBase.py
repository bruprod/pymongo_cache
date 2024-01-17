"""Base class for cache backends."""

from abc import abstractmethod
from typing import Any, Dict, Optional

from pymongo.collection import Collection

from cache_backend.QueryInfo import QueryInfo


class CacheBackendBase:
    """Base class for cache backends."""
    collection: Collection = None
    max_item_size: int = 0
    ttl: int = 0
    max_num_items: int = 0

    def __init__(self, collection: Collection, ttl: int = 0, max_item_size: int = 1*10**6, max_num_items: int = 1000):
        self.collection = collection
        self.max_item_size = max_item_size
        self.max_num_items = max_num_items
        self.ttl = ttl

    @abstractmethod
    def has_item(self, key: QueryInfo) -> bool:
        """Check if the cache has the item."""
        pass

    @abstractmethod
    def get(self, key: QueryInfo) -> Optional[Any]:
        """Get the value from the cache."""
        pass

    @abstractmethod
    def set(self, key: QueryInfo, value: Any, ttl: Optional[int] = None) -> None:
        """Set the value in the cache."""
        pass

    @abstractmethod
    def delete(self, key: QueryInfo) -> None:
        """Delete the value from the cache."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear the cache."""
        pass

    @abstractmethod
    def get_all(self) -> Dict[QueryInfo, Any]:
        """Get all the values from the cache."""
        pass

    def get_ttl(self) -> Optional[int]:
        """Get the TTL for the key."""
        return self.ttl

    def set_ttl(self, ttl: int) -> None:
        """Set the TTL for the key."""
        self.ttl = ttl

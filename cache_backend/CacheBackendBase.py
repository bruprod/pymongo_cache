"""Base class for cache backends."""

from abc import abstractmethod
from typing import Any, Dict, Optional

from pymongo.collection import Collection

from cache_backend.QueryInfo import QueryInfo


class CacheBackendBase:
    """Base class for cache backends."""
    collection: Collection = None

    def __init__(self, collection: Collection):
        self.collection = collection

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
    def get_ttl(self, key: QueryInfo) -> Optional[int]:
        """Get the TTL for the key."""
        pass

    @abstractmethod
    def set_ttl(self, key: QueryInfo, ttl: int) -> None:
        """Set the TTL for the key."""
        pass

    @abstractmethod
    def get_all(self) -> Dict[QueryInfo, Any]:
        """Get all the values from the cache."""
        pass

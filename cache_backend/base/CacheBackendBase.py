"""Base class for cache backends."""
import time
from abc import abstractmethod, ABCMeta
from threading import Thread
from typing import Any, Dict, Optional, Tuple

from pymongo.collection import Collection

from cache_backend.QueryInfo import QueryInfo

_cache_backend_registry: Dict[Tuple[str, str], "CacheBackendBase"] = {}


class CacheBackendBase(metaclass=ABCMeta):
    """Base class for cache backends."""

    collection: Collection = None
    max_item_size: int = 0
    ttl: int = 0
    max_num_items: int = 0
    _cache_cleanup_thread: Any = None
    _cache_cleanup_cycle_time: float = 0  # In seconds
    _cache_cleanup_handler = None

    def __init__(
        self,
        collection: Collection,
        ttl: int = 0,
        max_item_size: int = 1 * 10**6,
        max_num_items: int = 1000,
        cache_cleanup_cycle_time: Optional[float] = None,
    ):
        self.collection = collection
        self.max_item_size = max_item_size
        self.max_num_items = max_num_items
        self.ttl = ttl
        if cache_cleanup_cycle_time is not None:
            self._cache_cleanup_thread = Thread(target=self._cache_cleanup, daemon=True)
            self._cache_cleanup_thread.start()
            self._cache_cleanup_cycle_time = cache_cleanup_cycle_time

        _cache_backend_registry[(collection.database.name, collection.name)] = self

    @abstractmethod
    def get(self, key: QueryInfo) -> Optional[Any]:
        """Get the value from the cache."""
        pass

    @abstractmethod
    def set(
        self,
        key: QueryInfo,
        value: Any,
        execution_time_millis: float,
        ttl: Optional[int] = None,
    ) -> None:
        """Set the value in the cache.
        :param ttl: The time to live for the key.
        :param value: The value to set.
        :param key: The key to set.
        :param execution_time_millis: The execution time of the query in milliseconds.
        """
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

    @abstractmethod
    def _cache_cleanup_internal(self) -> None:
        """Clean up the cache."""
        pass

    def _cache_cleanup(self) -> None:
        """Clean up the cache."""
        while True:
            if self._cache_cleanup_handler is not None:
                print("Running cache cleanup.")
                self._cache_cleanup_internal()
            time.sleep(self._cache_cleanup_cycle_time)

    @staticmethod
    def clear_cache_for_database_and_collection(
        collection_name: str, database_name: str
    ) -> None:
        """Clear the cache for the database and collection."""
        _cache_backend_registry[(database_name, collection_name)].clear()

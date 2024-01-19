"""Implements a handler for the cleanup of the cache."""
import enum
from abc import abstractmethod, ABCMeta
from typing import List

from pymongo.collection import Collection

from cache_backend.QueryInfo import QueryInfo


class CleanupStrategy(enum.Enum):
    """Defines the cleanup strategy for the cache."""
    LRU = 0
    LFU = 1
    EXECUTION_TIME = 2


class CacheCleanupHandlerBase(metaclass=ABCMeta):
    """Implements a handler for the cleanup of the cache."""
    _collection: Collection = None
    _max_item_size: int = 0
    _max_num_items: int = 0

    def __init__(self, collection: Collection, max_item_size: int = 1 * 10 ** 6, max_num_items: int = 1000,
                 cleanup_strategy: CleanupStrategy = CleanupStrategy.LRU):
        self._collection = collection
        self._max_item_size = max_item_size
        self._max_num_items = max_num_items
        self._cleanup_strategy = cleanup_strategy

    def _get_entries_to_remove_from_cache(self, entries_to_cleanup: int) -> List[QueryInfo]:
        """Get the entries to remove from the cache."""
        if self._cleanup_strategy == CleanupStrategy.LRU:
            return self.get_n_oldest_entries(entries_to_cleanup)
        elif self._cleanup_strategy == CleanupStrategy.LFU:
            return self.get_n_least_frequent_entries(entries_to_cleanup)
        elif self._cleanup_strategy == CleanupStrategy.EXECUTION_TIME:
            return self.get_n_fastest_entries(entries_to_cleanup)

    def cleanup_cache(self):
        entries_to_cleanup = self.get_elements_in_cache() - self._max_num_items
        if entries_to_cleanup <= 0:
            return

        entries_to_remove = self._get_entries_to_remove_from_cache(entries_to_cleanup)
        self.delete_entries(entries_to_remove)

    @abstractmethod
    def get_elements_in_cache(self) -> int:
        """
        Get the elements in the cache.
        :return: The current number of elements in the cache
        """
        pass

    @abstractmethod
    def get_n_oldest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n oldest entries in the cache.
        :param n: The number of entries to get.
        :return: The n oldest entries in the cache.
        """
        pass

    @abstractmethod
    def get_n_least_frequent_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n least frequent entries in the cache.
        :param n: The number of entries to get.
        :return: The n least frequent entries in the cache.
        """
        pass

    @abstractmethod
    def get_n_fastest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n fastest entries in the cache.
        :param n: The number of entries to get.
        :return: The n fastest entries in the cache.
        """
        pass

    @abstractmethod
    def delete_entries(self, entries: List[QueryInfo]) -> None:
        """
        Delete the entries from the cache.
        :param entries: The entries to delete.
        """
        pass

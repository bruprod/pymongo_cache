"""Implements the cleanup handler for the in-memory cache."""
from typing import List, Dict

from pymongo.collection import Collection

from cache_backend.base.CacheCleanupHandlerBase import (
    CleanupStrategy,
    CacheCleanupHandlerBase,
)
from cache_backend.CacheEntry import CacheEntry
from cache_backend.QueryInfo import QueryInfo


class InMemoryCacheCleanupHandler(CacheCleanupHandlerBase):
    """Implements the cleanup handler for the in-memory cache."""

    _cache: Dict[QueryInfo, CacheEntry] = {}

    def __init__(
        self,
        collection: Collection,
        max_item_size: int = 1 * 10**6,
        max_num_items: int = 1000,
        cleanup_strategy: CleanupStrategy = CleanupStrategy.LRU,
    ):
        super().__init__(collection, max_item_size, max_num_items, cleanup_strategy)
        self._cache = {}

    def get_elements_in_cache(self) -> int:
        """
        Get the elements in the cache.
        :return: The current number of elements in the cache
        """
        return len(self._cache)

    def get_n_oldest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n oldest entries in the cache.
        :param n: The number of entries to get.
        :return: The n oldest entries in the cache.
        """
        entries = list(self._cache.values())
        entries.sort(key=lambda x: x.timestamp)
        entries = [entry.query_info for entry in entries[:n]]
        return entries

    def get_n_least_frequent_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n least frequent entries in the cache.
        :param n: The number of entries to get.
        :return: The n least frequent entries in the cache.
        """
        entries = list(self._cache.values())
        entries.sort(key=lambda x: x.access_count)
        entries = [entry.query_info for entry in entries[:n]]
        return entries

    def get_n_fastest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n fastest entries in the cache.
        :param n: The number of entries to get.
        :return: The n fastest entries in the cache.
        """
        entries = list(self._cache.values())
        entries.sort(key=lambda x: x.execution_time)
        entries = [entry.query_info for entry in entries[:n]]
        return entries

    def delete_entries(self, entries_to_remove: List[QueryInfo]) -> None:
        """
        Delete the given entries from the cache.
        :param entries_to_remove: The entries to remove.
        """
        for entry in entries_to_remove:
            if entry in self._cache:
                del self._cache[entry]

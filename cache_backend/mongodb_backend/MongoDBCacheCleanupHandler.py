""" The cache cleanup handler for MongoDB. """
from typing import List

from pymongo.collection import Collection

from cache_backend.CacheCleanupHandlerBase import CacheCleanupHandlerBase, CleanupStrategy
from cache_backend.Constants import HASH_VAL, COLLECTION_NAME, EXECUTION_TIME, ACCESS_COUNT, TIMESTAMP, QUERY_INFO
from cache_backend.QueryInfo import QueryInfo


class MongoDBCacheCleanupHandler(CacheCleanupHandlerBase):
    """ The cache cleanup handler for MongoDB. """
    _cache_collection: Collection = None

    def __init__(self, collection: Collection, max_item_size: int = 1 * 10 ** 6, max_num_items: int = 1000,
                 cleanup_strategy: CleanupStrategy = CleanupStrategy.LRU, cache_collection: Collection = None):
        super().__init__(collection, max_item_size, max_num_items, cleanup_strategy)
        self._cache_collection = cache_collection

    def get_elements_in_cache(self) -> int:
        """
        Get the elements in the cache.
        :return: The current number of elements in the cache
        """
        return self._cache_collection.count_documents({})

    def get_n_oldest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n oldest entries in the cache.
        :param n: The number of entries to get.
        :return: The n oldest entries in the cache.
        """
        entries = list(self._cache_collection.find({}, projection={"_id": 0}).sort(TIMESTAMP, 1).limit(n))
        entries = [QueryInfo(**entry[QUERY_INFO]) for entry in entries]
        return entries

    def get_n_least_frequent_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n least frequent entries in the cache.
        :param n: The number of entries to get.
        :return: The n least frequent entries in the cache.
        """
        entries = list(self._cache_collection.find({}, projection={"_id": 0}).sort(ACCESS_COUNT, 1).limit(n))
        entries = [QueryInfo(**entry[QUERY_INFO]) for entry in entries]
        return entries

    def get_n_fastest_entries(self, n: int) -> List[QueryInfo]:
        """
        Get the n fastest entries in the cache.
        :param n: The number of entries to get.
        :return: The n fastest entries in the cache.
        """
        entries = list(self._cache_collection.find({}, projection={"_id": 0}).sort(EXECUTION_TIME, 1).limit(n))
        entries = [QueryInfo(**entry[QUERY_INFO]) for entry in entries]
        return entries

    def delete_entries(self, entries_to_remove: List[QueryInfo]) -> None:
        """
        Delete the given entries from the cache.
        :param entries_to_remove: The entries to remove.
        """
        self._cache_collection.delete_many({
            HASH_VAL: {"$in": [entry.__hash__() for entry in entries_to_remove]},
            COLLECTION_NAME: self._collection.name
        })

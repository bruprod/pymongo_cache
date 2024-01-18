""" Class for caching MongoDB queries in a SQLite database. """
import atexit
from typing import Any, Dict

from pymongo import IndexModel, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from cache_backend.CacheBackendBase import CacheBackendBase
from cache_backend.CacheEntry import CacheEntry
from cache_backend.Constants import VALUE, QUERY_INFO, CACHE_DATABASE, CACHE_ENTRIES, COLLECTION_NAME, \
    HASH_VAL
from cache_backend.QueryInfo import QueryInfo


class MongoDBCacheBackend(CacheBackendBase):
    """ Class for caching MongoDB queries in a Mongodb database. """
    _cache_collection = None

    def __init__(self, collection: Collection, ttl: int = 0, max_item_size: int = 1 * 10 ** 6,
                 max_num_items: int = 1000):
        super().__init__(collection, ttl, max_item_size, max_num_items)
        self._cache_collection = self._get_cache_collection()

        # Register the clear function to be called when the program exits
        atexit.register(self.clear)

    def _get_cache_collection(self) -> Collection:
        """ Create the table if it doesn't exist. """
        client = self.collection.database.client
        db = Database(client, CACHE_DATABASE)
        coll = Collection(db, CACHE_ENTRIES)
        coll.create_indexes([IndexModel([(COLLECTION_NAME, ASCENDING), (HASH_VAL, ASCENDING)], name="CacheIndex")])
        return coll

    def get(self, key: QueryInfo) -> Any:
        """ Get the value from the cache. """
        entry = self._cache_collection.find_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})
        if entry is not None:
            return entry[VALUE]
        return None

    def set(self, key: QueryInfo, value: Any, execution_time_millis, ttl: int = None) -> None:
        """ Set the value in the cache.
        :param ttl: The time to live for the key.
        :param value: The value to set.
        :param key: The key to set.
        :param execution_time_millis: The execution time of the query in milliseconds.
        """

        cache_entry = CacheEntry(key, value, self.collection.name, key.__hash__(), execution_time_millis)
        self._cache_collection.update_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()},
                                          {"$set": cache_entry.to_dict()}, upsert=True,
                                          bypass_document_validation=True)

    def delete(self, key: QueryInfo) -> None:
        """ Delete the value from the cache. """
        self._cache_collection.delete_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})

    def clear(self) -> None:
        """ Clear the cache. """
        self._cache_collection.delete_many({COLLECTION_NAME: self.collection.name})

    def get_all(self) -> Dict[QueryInfo, Any]:
        """ Get all the values from the cache. """
        return {item[QUERY_INFO]: item[VALUE] for item in self.collection.find({})}

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()

    def __cache_cleanup(self) -> None:
        print("Cleaning up MongoDB cache backend.")

""" Class for caching MongoDB queries in a SQLite database. """
import atexit
from datetime import datetime
from typing import Any, Dict

from pymongo import IndexModel, ASCENDING, WriteConcern
from pymongo.collection import Collection
from pymongo.database import Database

from cache_backend.CacheEntry import CacheEntry
from cache_backend.Constants import (
    VALUE,
    QUERY_INFO,
    CACHE_DATABASE,
    CACHE_ENTRIES,
    COLLECTION_NAME,
    HASH_VAL,
    TIMESTAMP,
    ACCESS_COUNT,
)
from cache_backend.QueryInfo import QueryInfo
from cache_backend.base.CacheBackendBase import CacheBackendBase
from cache_backend.base.CacheCleanupHandlerBase import CleanupStrategy
from cache_backend.mongodb_backend.MongoDBCacheCleanupHandler import (
    MongoDBCacheCleanupHandler,
)


class MongoDBCacheBackend(CacheBackendBase):
    """Class for caching MongoDB queries in a Mongodb database."""

    _cache_collection = None

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
        self._cache_collection = self._get_cache_collection()

        self._cache_cleanup_handler = MongoDBCacheCleanupHandler(
            self.collection,
            max_item_size,
            max_num_items,
            cleanup_strategy=CleanupStrategy.LRU,
            cache_collection=self._cache_collection,
        )

        # Register the clear function to be called when the program exits
        atexit.register(self.clear)

    def _get_cache_collection(self) -> Collection:
        """Create the table if it doesn't exist."""
        client = self.collection.database.client
        db = Database(client, CACHE_DATABASE)
        coll = Collection(db, CACHE_ENTRIES)
        coll.create_indexes(
            [
                IndexModel(
                    [(COLLECTION_NAME, ASCENDING), (HASH_VAL, ASCENDING)],
                    name="CacheIndex",
                )
            ]
        )
        return coll

    def get(self, key: QueryInfo) -> Any:
        """Get the value from the cache."""
        entry = self._cache_collection.find_one_and_update(
            {COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()},
            {"$inc": {ACCESS_COUNT: 1}, "$set": {TIMESTAMP: datetime.now()}},
            return_document=True,
        )

        if entry is not None:
            return entry[VALUE]
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

        self._cache_cleanup_internal()

        cache_entry = CacheEntry(
            key, value, self.collection.name, key.__hash__(), execution_time_millis
        )

        # Do not wait for writing to be acknowledged, such that we don't slow down the query
        self._cache_collection.with_options(write_concern=WriteConcern(w=0)).insert_one(
            cache_entry.to_dict(), bypass_document_validation=True
        )

    def delete(self, key: QueryInfo) -> None:
        """Delete the value from the cache."""
        self._cache_collection.with_options(write_concern=WriteConcern(w=0)).delete_one(
            {COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()}
        )

    def clear(self) -> None:
        """Clear the cache."""
        self._cache_collection.with_options(
            write_concern=WriteConcern(w=0)
        ).delete_many({COLLECTION_NAME: self.collection.name})

    def get_all(self) -> Dict[QueryInfo, Any]:
        """Get all the values from the cache."""
        return {item[QUERY_INFO]: item[VALUE] for item in self.collection.find({})}

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()

    def _cache_cleanup_internal(self) -> None:
        """Clean up the cache."""
        self._cache_cleanup_handler.cleanup_cache()

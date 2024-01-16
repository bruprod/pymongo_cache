""" Class for caching MongoDB queries in a SQLite database. """
import json
from dataclasses import dataclass
from typing import Any, Dict

from pymongo import IndexModel, ASCENDING
from pymongo.collection import Collection

from cache_backend.CacheBackendBase import CacheBackendBase
from cache_backend.QueryInfo import QueryInfo

VALUE = "value"

QUERY_INFO = "query_info"

CACHE_DATABASE = "CacheDatabase"
CACHE_ENTRIES = "CacheEntries"
COLLECTION_NAME = "collection_name"
HASH_VAL = "hash_val"


@dataclass()
class CacheEntry:
    query_info: QueryInfo
    value: Any
    collection_name: str
    hash_val: int

    def to_dict(self):
        """Convert the entry to dict"""
        entry_dict = self.__dict__

        entry_dict[QUERY_INFO] = entry_dict[QUERY_INFO].__dict__

        return entry_dict


class MongoDBCacheBackend(CacheBackendBase):
    """ Class for caching MongoDB queries in a Mongodb database. """

    def __init__(self, collection: Collection):
        super().__init__(collection)

    def _get_cache_collection(self) -> Collection:
        """ Create the table if it doesn't exist. """
        client = self.collection.database.client
        db = client[CACHE_DATABASE]
        coll = Collection(db, CACHE_ENTRIES)
        coll.create_indexes([IndexModel([(COLLECTION_NAME, ASCENDING), (HASH_VAL, ASCENDING)], name="CacheIndex")])
        return coll

    def has_item(self, key: QueryInfo) -> bool:
        """ Check if the cache has the item. """
        coll = self._get_cache_collection()
        res = coll.find_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})
        return res is not None

    def get(self, key: QueryInfo) -> Any:
        """ Get the value from the cache. """
        entry = self._get_cache_collection().find_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})
        if entry is not None:
            return entry[VALUE]
        return None

    def set(self, key: QueryInfo, value: Any, ttl: int = None) -> None:
        """ Set the value in the cache. """

        cache_entry = CacheEntry(key, value, self.collection.name, key.__hash__())
        self._get_cache_collection().update_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()},
                                                {"$set": cache_entry.to_dict()}, upsert=True)

    def delete(self, key: QueryInfo) -> None:
        """ Delete the value from the cache. """
        self._get_cache_collection().delete_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})

    def clear(self) -> None:
        """ Clear the cache. """
        self._get_cache_collection().delete_many({COLLECTION_NAME: self.collection.name})

    def get_ttl(self, key: QueryInfo) -> int:
        """ Get the TTL for the key. """
        return self._get_cache_collection().find_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()})[
            "ttl"]

    def set_ttl(self, key: QueryInfo, ttl: int) -> None:
        """ Set the TTL for the key. """
        self._get_cache_collection().update_one({COLLECTION_NAME: self.collection.name, HASH_VAL: key.__hash__()},
                                                {"$set": {"ttl": ttl}})

    def get_all(self) -> Dict[QueryInfo, Any]:
        """ Get all the values from the cache. """
        return {item[QUERY_INFO]: item[VALUE] for item in self.collection.find({})}

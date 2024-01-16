"""Mongo database class with cache."""
from pymongo.database import Database

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.MongoCollectionWithCache import MongoCollectionWithCache


class MongoDatabaseWithCache(Database):
    """
    Mongo client class with cache.
    """
    _cache_backend: CacheBackend = CacheBackend.MEMORY

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.MEMORY, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = cache_backend

    def __getitem__(self, item):
        return MongoCollectionWithCache(self, item, cache_backend=self._cache_backend)

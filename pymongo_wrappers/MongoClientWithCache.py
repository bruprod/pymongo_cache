"""Mongo client class with cache."""
from pymongo import MongoClient

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.MongoDatabaseWithCache import MongoDatabaseWithCache


class MongoClientWithCache(MongoClient):
    """
    Mongo client class with cache.
    """
    _cache_backend: CacheBackend = CacheBackend.IN_MEMORY

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.IN_MEMORY, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = cache_backend

    def __getitem__(self, name: str) -> MongoDatabaseWithCache:
        return MongoDatabaseWithCache(self, name, cache_backend=self._cache_backend)

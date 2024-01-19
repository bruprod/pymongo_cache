"""Mongo client class with cache."""
from pymongo import MongoClient

from cache_backend.CacheBackend import CacheBackend
from pymongo_wrappers.CacheFunctions import DEFAULT_CACHE_FUNCTIONS
from pymongo_wrappers.MongoDatabaseWithCache import MongoDatabaseWithCache


class MongoClientWithCache(MongoClient):
    """
    Mongo client class with cache.
    """
    _cache_backend: CacheBackend = CacheBackend.IN_MEMORY
    _functions_to_cache = None
    _database_created = None
    _cache_cleanup_cycle_time = 5.

    def __init__(self, *args, cache_backend: CacheBackend = CacheBackend.IN_MEMORY,
                 functions_to_cache=None, cache_cleanup_cycle_time: float = 5., **kwargs):
        super().__init__(*args, **kwargs)
        self._cache_backend = cache_backend
        if functions_to_cache is None:
            self._functions_to_cache = DEFAULT_CACHE_FUNCTIONS
        else:
            self._functions_to_cache = functions_to_cache

        self._database_created = {}
        self._cache_cleanup_cycle_time = cache_cleanup_cycle_time

    def __getitem__(self, name: str) -> MongoDatabaseWithCache:
        if name in self._database_created:
            return self._database_created[name]

        db = MongoDatabaseWithCache(
            self,
            name,
            cache_backend=self._cache_backend,
            functions_to_cache=self._functions_to_cache,
            cache_cleanup_cycle_time=self._cache_cleanup_cycle_time
        )

        self._database_created[name] = db

        return db
